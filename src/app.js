const BASE_LINKS = [
    "https://kajarling.kajarling.ooguy.com/download/",
    "https://lugyiappreel.carton-lugyiapp.gleeze.com/download/",
    "https://lugyiapplication.nanmoelay.ooguy.com/download/",
    "https://lugi.samalay.ooguy.com/download/"
];

const DOMAIN_TAG = "lugyiapplication.vercel.app";
const MAX_SIZE = 300 * 1024 * 1024;

// DOM
const tabs = document.querySelectorAll('.tab');
const fileSection = document.getElementById('file-upload-section');
const urlSection = document.getElementById('url-upload-section');
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');
const urlInput = document.getElementById('url-input');
const urlUploadBtn = document.getElementById('url-upload-btn');
const progressSection = document.getElementById('progress-section');
const fileNameDisplay = document.getElementById('file-name-display');
const progressPercent = document.getElementById('progress-percent');
const progressFill = document.getElementById('progress-fill');
const uploadSpeed = document.getElementById('upload-speed');
const uploadStatus = document.getElementById('upload-status');
const resultSection = document.getElementById('result-section');
const linkGroup = document.getElementById('link-group');
const historyList = document.getElementById('history-list');
const clearHistoryBtn = document.getElementById('clear-history');

// Tab switch
tabs.forEach(tab => {
    tab.addEventListener('click', () => {
        tabs.forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        const mode = tab.dataset.mode;
        fileSection.classList.toggle('active', mode === 'file');
        urlSection.classList.toggle('active', mode === 'url');
    });
});

// Drop zone
dropZone.addEventListener('click', () => fileInput.click());
dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropZone.classList.add('drag-over');
});
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]);
});
fileInput.addEventListener('change', () => {
    if (fileInput.files.length) handleFile(fileInput.files[0]);
});

// URL upload
urlUploadBtn.addEventListener('click', () => {
    const url = urlInput.value.trim();
    if (!url) return alert("URL ထည့်ပါ");
    handleURLUpload(url);
});

// Unique filename
function generateUniqueFilename(originalName) {
    const ext = originalName.includes('.') ? '.' + originalName.split('.').pop() : '';
    const ts = Date.now().toString(36);
    const rand = Math.random().toString(36).substring(2, 10);
    return `${ts}-${rand}-${DOMAIN_TAG}${ext}`;
}

// File upload
async function handleFile(file) {
    if (file.size > MAX_SIZE) {
        return alert(`ဖိုင်အရွယ်အစား ${formatSize(file.size)} က 300MB ထက်ကျော်နေပါတယ်`);
    }

    const uniqueName = generateUniqueFilename(file.name);
    resetUI(uniqueName);

    try {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('filename', uniqueName);

        const xhr = new XMLHttpRequest();
        const startTime = Date.now();

        xhr.upload.addEventListener('progress', (e) => {
            if (e.lengthComputable) {
                const pct = Math.round((e.loaded / e.total) * 100);
                updateProgress(pct);
                const elapsed = (Date.now() - startTime) / 1000;
                if (elapsed > 0) {
                    uploadSpeed.textContent = `${formatSize(e.loaded / elapsed)}/s`;
                }
                uploadStatus.textContent = pct >= 100 ? "R2 ၂ခုကို တင်နေသည်..." : "Uploading...";
            }
        });

        await new Promise((resolve, reject) => {
            xhr.addEventListener('load', () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    resolve(JSON.parse(xhr.responseText));
                } else {
                    reject(new Error(xhr.responseText || `Upload failed: ${xhr.status}`));
                }
            });
            xhr.addEventListener('error', () => reject(new Error('Network error')));
            xhr.open('POST', '/api/upload');
            xhr.send(formData);
        });

        showResult(uniqueName);
    } catch (err) {
        alert("Upload failed: " + err.message);
        progressSection.classList.add('hidden');
    }
}

// URL upload
async function handleURLUpload(url) {
    let originalName = url.split('/').pop().split('?')[0] || 'file';
    const uniqueName = generateUniqueFilename(originalName);
    resetUI(uniqueName);
    uploadStatus.textContent = "Fetching remote file...";
    urlUploadBtn.disabled = true;

    try {
        const response = await fetch('/api/upload', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url, filename: uniqueName })
        });

        if (!response.ok && !response.body) {
            throw new Error(await response.text());
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop();

            for (const line of lines) {
                if (line.startsWith('data:')) {
                    try {
                        const data = JSON.parse(line.slice(5));
                        if (data.progress !== undefined) updateProgress(data.progress);
                        if (data.status) uploadStatus.textContent = data.status;
                        if (data.done) showResult(uniqueName);
                        if (data.error) throw new Error(data.error);
                    } catch (e) {
                        if (!e.message.includes('JSON') && !e.message.includes('Unexpected')) throw e;
                    }
                }
            }
        }
    } catch (err) {
        alert("Upload failed: " + err.message);
        progressSection.classList.add('hidden');
    } finally {
        urlUploadBtn.disabled = false;
    }
}

function resetUI(name) {
    resultSection.classList.add('hidden');
    progressSection.classList.remove('hidden');
    fileNameDisplay.textContent = name;
    updateProgress(0);
    uploadSpeed.textContent = '';
    uploadStatus.textContent = "Uploading...";
}

function updateProgress(pct) {
    progressFill.style.width = pct + '%';
    progressPercent.textContent = pct + '%';
}

function showResult(filename) {
    updateProgress(100);
    uploadStatus.textContent = "Complete!";

    const links = BASE_LINKS.map(base => base + filename);
    linkGroup.innerHTML = '';
    links.forEach(link => {
        const div = document.createElement('div');
        div.className = 'link-item';
        div.innerHTML = `
            <input type="text" value="${link}" readonly>
            <button class="btn-copy" onclick="copyLink(this, '${link}')">Copy</button>
        `;
        linkGroup.appendChild(div);
    });
    resultSection.classList.remove('hidden');
    saveToHistory(filename, links);
}

function copyLink(btn, link) {
    navigator.clipboard.writeText(link).then(() => {
        btn.textContent = 'Copied!';
        btn.classList.add('copied');
        setTimeout(() => {
            btn.textContent = 'Copy';
            btn.classList.remove('copied');
        }, 2000);
    });
}

// History
function getHistory() {
    try { return JSON.parse(localStorage.getItem('upload_history') || '[]'); }
    catch { return []; }
}

function saveToHistory(filename, links) {
    const history = getHistory();
    history.unshift({ filename, links, time: new Date().toLocaleString() });
    if (history.length > 50) history.length = 50;
    localStorage.setItem('upload_history', JSON.stringify(history));
    renderHistory();
}

function renderHistory() {
    const history = getHistory();
    if (history.length === 0) {
        historyList.innerHTML = '<p class="empty-history">No uploads yet</p>';
        clearHistoryBtn.classList.add('hidden');
        return;
    }
    clearHistoryBtn.classList.remove('hidden');
    historyList.innerHTML = '';
    history.forEach(item => {
        const div = document.createElement('div');
        div.className = 'history-item';
        let linksHTML = item.links.map(link => `
            <div class="history-link-row">
                <input type="text" value="${link}" readonly>
                <button class="btn-copy" onclick="copyLink(this, '${link}')">Copy</button>
            </div>
        `).join('');
        div.innerHTML = `
            <div class="history-item-time">${item.time}</div>
            <div class="history-item-name">${item.filename}</div>
            <div class="history-links">${linksHTML}</div>
        `;
        historyList.appendChild(div);
    });
}

clearHistoryBtn.addEventListener('click', () => {
    if (confirm('History အားလုံး ဖျက်မလား?')) {
        localStorage.removeItem('upload_history');
        renderHistory();
    }
});

function formatSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

renderHistory();
