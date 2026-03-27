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

    const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB per chunk (S3 multipart minimum)

    try {
        // ===================== JSON body = Remote URL upload =====================
        if (contentType.includes('application/json')) {
            const { url, filename } = await request.json();

            const { readable, writable } = new TransformStream();
            const writer = writable.getWriter();
            const encoder = new TextEncoder();
            const send = async (d) => await writer.write(encoder.encode(`data:${JSON.stringify(d)}\n`));

            (async () => {
                try {
                    await send({ status: "Remote ဖိုင် ဆွဲယူနေသည်...", progress: 2 });

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
                        await send({ error: "Remote response has no body" });
                        await writer.close();
                        return;
                    }

                    await send({ status: "Multipart upload စနေသည်...", progress: 5 });

                    // Initiate multipart uploads on both R2
                    const [upload1, upload2] = await Promise.all([
                        initiateMultipart(r2Accounts[0], filename, mime),
                        initiateMultipart(r2Accounts[1], filename, mime),
                    ]);

                    const parts1 = [];
                    const parts2 = [];
                    let partNumber = 0;
                    let totalRead = 0;
                    let buffer = new Uint8Array(0);

                    const reader = remote.body.getReader();

                    while (true) {
                        const { done, value } = await reader.read();

                        if (value) {
                            // buffer ကို append
                            const newBuf = new Uint8Array(buffer.length + value.length);
                            newBuf.set(buffer, 0);
                            newBuf.set(value, buffer.length);
                            buffer = newBuf;
                            totalRead += value.length;
                        }

                        // chunk ပြည့်ရင် (သို့) stream ပြီးရင် upload
                        while (buffer.length >= CHUNK_SIZE || (done && buffer.length > 0)) {
                            const slice = buffer.slice(0, CHUNK_SIZE);
                            buffer = buffer.slice(CHUNK_SIZE);
                            partNumber++;

                            const pct = cl > 0
                                ? Math.min(90, Math.round((totalRead / cl) * 90))
                                : Math.min(90, 5 + partNumber * 5);

                            await send({ status: `Part ${partNumber} တင်နေသည်... (${formatBytes(totalRead)})`, progress: pct });

                            // Upload part to both R2 concurrently
                            const [res1, res2] = await Promise.all([
                                uploadPart(r2Accounts[0], filename, upload1, partNumber, slice),
                                uploadPart(r2Accounts[1], filename, upload2, partNumber, slice),
                            ]);

                            parts1.push({ PartNumber: partNumber, ETag: res1 });
                            parts2.push({ PartNumber: partNumber, ETag: res2 });

                            if (done && buffer.length === 0) break;
                        }

                        if (done) break;
                    }

                    // PartNumber 0 ဆိုရင် ဖိုင်ဗလာ
                    if (partNumber === 0) {
                        await Promise.all([
                            abortMultipart(r2Accounts[0], filename, upload1),
                            abortMultipart(r2Accounts[1], filename, upload2),
                        ]);
                        await send({ error: "ဖိုင်ဗလာ ဖြစ်နေပါတယ်" });
                        await writer.close();
                        return;
                    }

                    await send({ status: "Multipart upload ပြီးဆုံးနေသည်...", progress: 95 });

                    // Complete multipart uploads
                    await Promise.all([
                        completeMultipart(r2Accounts[0], filename, upload1, parts1),
                        completeMultipart(r2Accounts[1], filename, upload2, parts2),
                    ]);

                    await send({ status: "Complete!", progress: 100, done: true, filename });

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

        // ===================== FormData = Direct file upload =====================
        if (contentType.includes('multipart/form-data')) {
            const formData = await request.formData();
            const file = formData.get('file');
            const filename = formData.get('filename');

            if (!file) return jsonResp({ error: 'No file' }, 400);
            if (file.size > 300 * 1024 * 1024) return jsonResp({ error: 'File exceeds 300MB' }, 400);

            const mime = file.type || 'application/octet-stream';

            // Small file: single PUT (< 5MB)
            if (file.size <= CHUNK_SIZE) {
                const fileBuffer = await file.arrayBuffer();
                const results = await Promise.allSettled([
                    uploadToR2Single(r2Accounts[0], filename, fileBuffer, mime),
                    uploadToR2Single(r2Accounts[1], filename, fileBuffer, mime),
                ]);
                const errors = results.filter(r => r.status === 'rejected').map(r => r.reason.message);
                if (errors.length === 2) return jsonResp({ error: `Both R2 failed: ${errors.join(' | ')}` }, 500);
                return jsonResp({ success: true, filename, warnings: errors.length ? errors : undefined });
            }

            // Large file: multipart
            const [upload1, upload2] = await Promise.all([
                initiateMultipart(r2Accounts[0], filename, mime),
                initiateMultipart(r2Accounts[1], filename, mime),
            ]);

            const parts1 = [];
            const parts2 = [];
            let partNumber = 0;
            let buffer = new Uint8Array(0);
            const reader = file.stream().getReader();

            while (true) {
                const { done, value } = await reader.read();
                if (value) {
                    const newBuf = new Uint8Array(buffer.length + value.length);
                    newBuf.set(buffer, 0);
                    newBuf.set(value, buffer.length);
                    buffer = newBuf;
                }
                while (buffer.length >= CHUNK_SIZE || (done && buffer.length > 0)) {
                    const slice = buffer.slice(0, CHUNK_SIZE);
                    buffer = buffer.slice(CHUNK_SIZE);
                    partNumber++;
                    const [res1, res2] = await Promise.all([
                        uploadPart(r2Accounts[0], filename, upload1, partNumber, slice),
                        uploadPart(r2Accounts[1], filename, upload2, partNumber, slice),
                    ]);
                    parts1.push({ PartNumber: partNumber, ETag: res1 });
                    parts2.push({ PartNumber: partNumber, ETag: res2 });
                    if (done && buffer.length === 0) break;
                }
                if (done) break;
            }

            await Promise.all([
                completeMultipart(r2Accounts[0], filename, upload1, parts1),
                completeMultipart(r2Accounts[1], filename, upload2, parts2),
            ]);

            return jsonResp({ success: true, filename });
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

function formatBytes(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

// ===================== S3 Multipart Upload Functions =====================

async function initiateMultipart(account, filename, contentType) {
    const host = `${account.accountId}.r2.cloudflarestorage.com`;
    const path = `/${account.bucketName}/${encodeURIComponent(filename)}`;
    const url = `https://${host}${path}?uploads`;
    const now = new Date();
    const dateStamp = toDateStamp(now);
    const amzDate = toAmzDate(now);
    const payloadHash = await sha256Hex('');

    const canonicalHeaders =
        `content-type:${contentType}\n` +
        `host:${host}\n` +
        `x-amz-content-sha256:${payloadHash}\n` +
        `x-amz-date:${amzDate}\n`;
    const signedHeaders = 'content-type;host;x-amz-content-sha256;x-amz-date';

    const canonicalRequest = [
        'POST', path, 'uploads=', canonicalHeaders, signedHeaders, payloadHash
    ].join('\n');

    const auth = await buildAuth(account, canonicalRequest, dateStamp, amzDate, signedHeaders);

    const resp = await fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': contentType,
            'X-Amz-Content-Sha256': payloadHash,
            'X-Amz-Date': amzDate,
            'Authorization': auth,
        }
    });

    if (!resp.ok) {
        const errBody = await resp.text();
        throw new Error(`${account.name} initiate multipart failed (${resp.status}): ${errBody}`);
    }

    const xml = await resp.text();
    const match = xml.match(/<UploadId>(.+?)<\/UploadId>/);
    if (!match) throw new Error(`${account.name} no UploadId in response`);
    return match[1];
}

async function uploadPart(account, filename, uploadId, partNumber, bodyBuffer) {
    const host = `${account.accountId}.r2.cloudflarestorage.com`;
    const path = `/${account.bucketName}/${encodeURIComponent(filename)}`;
    const queryString = `partNumber=${partNumber}&uploadId=${encodeURIComponent(uploadId)}`;
    const url = `https://${host}${path}?${queryString}`;
    const now = new Date();
    const dateStamp = toDateStamp(now);
    const amzDate = toAmzDate(now);
    const payloadHash = await sha256Hex(bodyBuffer);

    const canonicalHeaders =
        `host:${host}\n` +
        `x-amz-content-sha256:${payloadHash}\n` +
        `x-amz-date:${amzDate}\n`;
    const signedHeaders = 'host;x-amz-content-sha256;x-amz-date';

    // Query string parameters must be sorted
    const sortedQS = `partNumber=${partNumber}&uploadId=${encodeURIComponent(uploadId)}`;

    const canonicalRequest = [
        'PUT', path, sortedQS, canonicalHeaders, signedHeaders, payloadHash
    ].join('\n');

    const auth = await buildAuth(account, canonicalRequest, dateStamp, amzDate, signedHeaders);

    const resp = await fetch(url, {
        method: 'PUT',
        headers: {
            'X-Amz-Content-Sha256': payloadHash,
            'X-Amz-Date': amzDate,
            'Authorization': auth,
            'Content-Length': String(bodyBuffer.byteLength),
        },
        body: bodyBuffer,
    });

    if (!resp.ok) {
        const errBody = await resp.text();
        throw new Error(`${account.name} part ${partNumber} failed (${resp.status}): ${errBody}`);
    }

    const etag = resp.headers.get('ETag');
    if (!etag) throw new Error(`${account.name} part ${partNumber} no ETag`);
    return etag;
}

async function completeMultipart(account, filename, uploadId, parts) {
    const host = `${account.accountId}.r2.cloudflarestorage.com`;
    const path = `/${account.bucketName}/${encodeURIComponent(filename)}`;
    const queryString = `uploadId=${encodeURIComponent(uploadId)}`;
    const url = `https://${host}${path}?${queryString}`;
    const now = new Date();
    const dateStamp = toDateStamp(now);
    const amzDate = toAmzDate(now);

    const xmlBody = `<CompleteMultipartUpload>${parts.map(p =>
        `<Part><PartNumber>${p.PartNumber}</PartNumber><ETag>${p.ETag}</ETag></Part>`
    ).join('')}</CompleteMultipartUpload>`;

    const payloadHash = await sha256Hex(xmlBody);

    const canonicalHeaders =
        `content-type:application/xml\n` +
        `host:${host}\n` +
        `x-amz-content-sha256:${payloadHash}\n` +
        `x-amz-date:${amzDate}\n`;
    const signedHeaders = 'content-type;host;x-amz-content-sha256;x-amz-date';

    const canonicalRequest = [
        'POST', path, queryString, canonicalHeaders, signedHeaders, payloadHash
    ].join('\n');

    const auth = await buildAuth(account, canonicalRequest, dateStamp, amzDate, signedHeaders);

    const resp = await fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/xml',
            'X-Amz-Content-Sha256': payloadHash,
            'X-Amz-Date': amzDate,
            'Authorization': auth,
        },
        body: xmlBody,
    });

    if (!resp.ok) {
        const errBody = await resp.text();
        throw new Error(`${account.name} complete multipart failed (${resp.status}): ${errBody}`);
    }
}

async function abortMultipart(account, filename, uploadId) {
    try {
        const host = `${account.accountId}.r2.cloudflarestorage.com`;
        const path = `/${account.bucketName}/${encodeURIComponent(filename)}`;
        const queryString = `uploadId=${encodeURIComponent(uploadId)}`;
        const url = `https://${host}${path}?${queryString}`;
        const now = new Date();
        const dateStamp = toDateStamp(now);
        const amzDate = toAmzDate(now);
        const payloadHash = await sha256Hex('');

        const canonicalHeaders =
            `host:${host}\n` +
            `x-amz-content-sha256:${payloadHash}\n` +
            `x-amz-date:${amzDate}\n`;
        const signedHeaders = 'host;x-amz-content-sha256;x-amz-date';

        const canonicalRequest = [
            'DELETE', path, queryString, canonicalHeaders, signedHeaders, payloadHash
        ].join('\n');

        const auth = await buildAuth(account, canonicalRequest, dateStamp, amzDate, signedHeaders);

        await fetch(url, {
            method: 'DELETE',
            headers: {
                'X-Amz-Content-Sha256': payloadHash,
                'X-Amz-Date': amzDate,
                'Authorization': auth,
            }
        });
    } catch (e) {
        // ignore abort errors
    }
}

// ===================== Single PUT upload (small files) =====================
async function uploadToR2Single(account, filename, body, contentType) {
    const host = `${account.accountId}.r2.cloudflarestorage.com`;
    const path = `/${account.bucketName}/${encodeURIComponent(filename)}`;
    const url = `https://${host}${path}`;
    const now = new Date();
    const dateStamp = toDateStamp(now);
    const amzDate = toAmzDate(now);
    const payloadHash = await sha256Hex(body);

    const canonicalHeaders =
        `content-type:${contentType}\n` +
        `host:${host}\n` +
        `x-amz-content-sha256:${payloadHash}\n` +
        `x-amz-date:${amzDate}\n`;
    const signedHeaders = 'content-type;host;x-amz-content-sha256;x-amz-date';

    const canonicalRequest = [
        'PUT', path, '', canonicalHeaders, signedHeaders, payloadHash
    ].join('\n');

    const auth = await buildAuth(account, canonicalRequest, dateStamp, amzDate, signedHeaders);

    const resp = await fetch(url, {
        method: 'PUT',
        headers: {
            'Content-Type': contentType,
            'X-Amz-Content-Sha256': payloadHash,
            'X-Amz-Date': amzDate,
            'Authorization': auth,
        },
        body: body,
    });

    if (!resp.ok) {
        const errBody = await resp.text();
        throw new Error(`${account.name} upload failed (${resp.status}): ${errBody}`);
    }
}

// ===================== Auth builder =====================
async function buildAuth(account, canonicalRequest, dateStamp, amzDate, signedHeaders) {
    const region = 'auto';
    const service = 's3';
    const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`;

    const stringToSign = [
        'AWS4-HMAC-SHA256',
        amzDate,
        credentialScope,
        await sha256Hex(new TextEncoder().encode(canonicalRequest))
    ].join('\n');

    const signingKey = await getSignatureKey(account.secretAccessKey, dateStamp, region, service);
    const signature = await hmacHex(signingKey, stringToSign);

    return `AWS4-HMAC-SHA256 Credential=${account.accessKeyId}/${credentialScope}, ` +
           `SignedHeaders=${signedHeaders}, Signature=${signature}`;
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
