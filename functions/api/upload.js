export async function onRequestPost(context) {
    const { request, env } = context;
    const contentType = request.headers.get('content-type') || '';

    const r2Accounts = [
        {
            name: "R2_1",
            accountId: env.R2_1_ACCOUNT_ID,
            accessKeyId: env.R2_1_ACCESS_KEY_ID,
            secretAccessKey: env.R2_1_SECRET_ACCESS_KEY,
            bucketName: env.R2_1_BUCKET_NAME,
        },
        {
            name: "R2_2",
            accountId: env.R2_2_ACCOUNT_ID,
            accessKeyId: env.R2_2_ACCESS_KEY_ID,
            secretAccessKey: env.R2_2_SECRET_ACCESS_KEY,
            bucketName: env.R2_2_BUCKET_NAME,
        }
    ];

    try {
        // ===================== JSON body = Remote URL upload (STREAMING) =====================
        if (contentType.includes('application/json')) {
            const { url, filename } = await request.json();

            const { readable, writable } = new TransformStream();
            const writer = writable.getWriter();
            const encoder = new TextEncoder();
            const send = async (d) => await writer.write(encoder.encode(`data:${JSON.stringify(d)}\n`));

            (async () => {
                try {
                    await send({ status: "Remote ဖိုင် ဆွဲယူနေသည်...", progress: 5 });

                    const remote = await fetch(url, {
                        headers: { 'User-Agent': 'Mozilla/5.0' }
                    });
                    if (!remote.ok) {
                        await send({ error: `Remote fetch failed: ${remote.status}` });
                        await writer.close();
                        return;
                    }

                    const cl = parseInt(remote.headers.get('content-length') || '0');
                    if (cl > 300 * 1024 * 1024) {
                        await send({ error: "ဖိုင် 300MB ထက်ကျော်နေပါတယ်" });
                        await writer.close();
                        return;
                    }

                    const mime = remote.headers.get('content-type') || 'application/octet-stream';

                    if (!remote.body) {
                        await send({ error: "Remote response has no body stream" });
                        await writer.close();
                        return;
                    }

                    // Stream ကို tee() နဲ့ နှစ်ခွပြီး R2 နှစ်ခုဆီ တစ်ပြိုင်နက်ပို့မယ်
                    const [stream1, stream2] = remote.body.tee();

                    await send({ status: "R2 #1 & #2 သို့ streaming တင်နေသည်...", progress: 20 });

                    // R2 နှစ်ခုကို တစ်ပြိုင်နက် streaming upload
                    const results = await Promise.allSettled([
                        streamUploadToR2(r2Accounts[0], filename, stream1, mime, cl || undefined),
                        streamUploadToR2(r2Accounts[1], filename, stream2, mime, cl || undefined),
                    ]);

                    const errors = results
                        .filter(r => r.status === 'rejected')
                        .map(r => r.reason.message);

                    if (errors.length === 2) {
                        await send({ error: `R2 နှစ်ခုလုံး fail: ${errors.join(' | ')}` });
                    } else if (errors.length === 1) {
                        await send({ status: "Complete! (warning: one R2 failed)", progress: 100, done: true, warnings: errors });
                    } else {
                        await send({ status: "Complete!", progress: 100, done: true });
                    }

                } catch (err) {
                    await send({ error: err.message });
                } finally {
                    await writer.close();
                }
            })();

            return new Response(readable, {
                headers: {
                    'Content-Type': 'text/event-stream',
                    'Cache-Control': 'no-cache',
                }
            });
        }

        // ===================== FormData = Direct file upload (STREAMING) =====================
        if (contentType.includes('multipart/form-data')) {
            const formData = await request.formData();
            const file = formData.get('file');
            const filename = formData.get('filename');

            if (!file) {
                return jsonResp({ error: 'No file' }, 400);
            }
            if (file.size > 300 * 1024 * 1024) {
                return jsonResp({ error: 'File exceeds 300MB' }, 400);
            }

            const mime = file.type || 'application/octet-stream';
            const fileSize = file.size;

            // File ကို stream အဖြစ်ပြောင်းပြီး tee() နဲ့ နှစ်ခွ
            const [stream1, stream2] = file.stream().tee();

            const results = await Promise.allSettled([
                streamUploadToR2(r2Accounts[0], filename, stream1, mime, fileSize),
                streamUploadToR2(r2Accounts[1], filename, stream2, mime, fileSize),
            ]);

            const errors = results
                .filter(r => r.status === 'rejected')
                .map(r => r.reason.message);

            if (errors.length === 2) {
                return jsonResp({ error: `Both R2 failed: ${errors.join(' | ')}` }, 500);
            }

            return jsonResp({
                success: true,
                filename,
                warnings: errors.length ? errors : undefined
            });
        }

        return new Response('Invalid request', { status: 400 });

    } catch (err) {
        return jsonResp({ error: err.message }, 500);
    }
}

function jsonResp(data, status = 200) {
    return new Response(JSON.stringify(data), {
        status,
        headers: { 'Content-Type': 'application/json' }
    });
}

// ===================== Streaming R2 Upload (UNSIGNED-PAYLOAD) =====================
async function streamUploadToR2(account, filename, bodyStream, contentType, contentLength) {
    const host = `${account.accountId}.r2.cloudflarestorage.com`;
    const url = `https://${host}/${account.bucketName}/${encodeURIComponent(filename)}`;
    const now = new Date();
    const dateStamp = toDateStamp(now);
    const amzDate = toAmzDate(now);
    const region = 'auto';
    const service = 's3';

    // Streaming ဖြစ်တဲ့အတွက် payload hash ကို UNSIGNED-PAYLOAD သုံးမယ်
    const payloadHash = 'UNSIGNED-PAYLOAD';

    const canonicalHeaders =
        `content-type:${contentType}\n` +
        `host:${host}\n` +
        `x-amz-content-sha256:${payloadHash}\n` +
        `x-amz-date:${amzDate}\n`;

    const signedHeaders = 'content-type;host;x-amz-content-sha256;x-amz-date';

    const canonicalRequest = [
        'PUT',
        `/${account.bucketName}/${encodeURIComponent(filename)}`,
        '',
        canonicalHeaders,
        signedHeaders,
        payloadHash
    ].join('\n');

    const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`;

    const stringToSign = [
        'AWS4-HMAC-SHA256',
        amzDate,
        credentialScope,
        await sha256Hex(new TextEncoder().encode(canonicalRequest))
    ].join('\n');

    const signingKey = await getSignatureKey(account.secretAccessKey, dateStamp, region, service);
    const signature = await hmacHex(signingKey, stringToSign);

    const authorization =
        `AWS4-HMAC-SHA256 Credential=${account.accessKeyId}/${credentialScope}, ` +
        `SignedHeaders=${signedHeaders}, Signature=${signature}`;

    const headers = {
        'Content-Type': contentType,
        'X-Amz-Content-Sha256': payloadHash,
        'X-Amz-Date': amzDate,
        'Authorization': authorization,
    };

    // content-length သိရင် ထည့်ပေးမယ် (R2 အတွက် ပိုကောင်းတယ်)
    if (contentLength) {
        headers['Content-Length'] = String(contentLength);
    }

    const resp = await fetch(url, {
        method: 'PUT',
        headers: headers,
        // Stream ကို body အဖြစ် တိုက်ရိုက်ပေး - memory ထဲ buffer မလုပ်တော့
        body: bodyStream,
        duplex: 'half',
    });

    if (!resp.ok) {
        const errBody = await resp.text();
        throw new Error(`${account.name} upload failed (${resp.status}): ${errBody}`);
    }
}

// ===================== AWS Sig V4 helpers =====================
function toDateStamp(d) {
    return d.toISOString().slice(0, 10).replace(/-/g, '');
}

function toAmzDate(d) {
    return d.toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z');
}

async function sha256Hex(data) {
    if (typeof data === 'string') data = new TextEncoder().encode(data);
    const hash = await crypto.subtle.digest('SHA-256', data);
    return bufToHex(hash);
}

async function hmacSign(key, data) {
    if (typeof key === 'string') key = new TextEncoder().encode(key);
    if (typeof data === 'string') data = new TextEncoder().encode(data);
    const cryptoKey = await crypto.subtle.importKey(
        'raw', key, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
    );
    return await crypto.subtle.sign('HMAC', cryptoKey, data);
}

async function hmacHex(key, data) {
    return bufToHex(await hmacSign(key, data));
}

async function getSignatureKey(secretKey, dateStamp, region, service) {
    const kDate = await hmacSign(new TextEncoder().encode('AWS4' + secretKey), dateStamp);
    const kRegion = await hmacSign(kDate, region);
    const kService = await hmacSign(kRegion, service);
    return await hmacSign(kService, 'aws4_request');
}

function bufToHex(buf) {
    return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
}
