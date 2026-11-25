const express = require('express');
const http = require('http');
const https = require('https');
const os = require('os');
const { exec } = require('child_process');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const AsteriskManager = require('asterisk-manager');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' }
});

// MySQL Connection Pools (kept small & queued to avoid "Too many connections" on busy servers)
const dbPool = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: "MYSQL_PASSWORD_PLACEHOLDER",
    database: 'asterisk',
    waitForConnections: true,
    connectionLimit: 3,
    queueLimit: 20,
    connectTimeout: 5000
});

const cdrDbPool = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: "MYSQL_PASSWORD_PLACEHOLDER",
    database: 'asteriskcdrdb',
    waitForConnections: true,
    connectionLimit: 2,
    queueLimit: 20,
    connectTimeout: 5000
});

const ami = new AsteriskManager(5038, "127.0.0.1", "admin", "AMI_PASSWORD_PLACEHOLDER", true);
ami.keepConnected();

// In-memory state stores
const peerStatuses = {};      // { peer: 'IDLE' | 'RINGING' | 'INCALL' }
const callStartTimes = {};    // { peer: timestamp_ms }
const callDetails = {};       // { peer: { dir: 'IN' | 'OUT', number: '017...', exten: '524', linkedid: '', bridgeid: '', calls: [] } }
const trunkActiveCalls = {};  // { trunkPeer: { [callKey]: { callKey, uniqueid, linkedid, bridgeid, dir, number, exten, status, startTime } } }
const regStatus = {};         // { peer: { registered: bool, ip: string, regSince: timestamp_ms } }
const channelMap = {};        // { uniqueid: { uniqueid, peer, channel, callerNum, exten, linkedid, bridgeid, context, isTrunk, state } }
const knownTrunks = new Set();
const trunkCanonicalMap = {};
const knownExtensions = new Set();

// Cached extensions & trunks list for resilience when DB is busy
let cachedExtensionsList = [];
let cachedTrunksList = [];

// Cached CDR call counts (overall + per-extension today)
let cachedCdrStats = {
    todayProcessed: 0,
    totalProcessed: 0,
    extDailyCalls: {}
};

// Cached server network & location metadata
let cachedPublicIp = 'Detecting...';
let cachedLocation = Intl.DateTimeFormat().resolvedOptions().timeZone || 'Local Server';
let prevCpuInfo = os.cpus();

// Normalize AMI command response (handles both Array and String safely)
function normalizeAmiOut(res) {
    if (!res) return '';
    const raw = res.content || res.output || '';
    return Array.isArray(raw) ? raw.join('\n') : String(raw);
}

// Validate real phone/extension number (rejects <unknown>, UUIDs, empty, 's', etc.)
function isValidNumber(val, excludeSelf = null) {
    if (!val) return false;
    const s = String(val).replace(/["<>]/g, '').trim();
    if (!s) return false;
    const lower = s.toLowerCase();
    if (['unknown', 's', 'asterisk', 'anonymous', 'private', 'unavailable', 'none'].includes(lower)) return false;
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s)) return false;
    if (excludeSelf && s === String(excludeSelf)) return false;
    return true;
}

// Extract dialed destination number from Dial application data or dialstring (e.g. "SIP/BOL/01713152687,300,T")
function extractDialedNumberFromData(appData = '') {
    if (!appData) return '';
    const m = String(appData).match(/^(?:SIP|PJSIP|IAX2|DAHDI|Local)\/[^/;,]+(?:\/([^,;/]+))?/i);
    if (m && m[1] && isValidNumber(m[1])) return m[1].trim();
    return '';
}

// Detect outgoing contexts/channels across Issabel/FreePBX dialplans
function isOutgoingContext(context = '', channel = '') {
    const ctx = String(context || '').toLowerCase();
    const ch = String(channel || '').toLowerCase();
    return (
        ctx.includes('dialout') ||
        ctx.includes('outbound') ||
        ctx.includes('from-internal') ||
        ctx.includes('macro-dialout') ||
        ctx.includes('sub-pincheck') ||
        ch.includes('dialout')
    );
}

// Refresh known extensions & trunks sets from MySQL pool
async function refreshPeerRegistry() {
    try {
        const [extRows] = await dbPool.execute('SELECT extension, name FROM users ORDER BY CAST(extension AS UNSIGNED) ASC');
        const [trunkRows] = await dbPool.execute('SELECT trunkid, name, tech, channelid FROM trunks WHERE disabled = "off" ORDER BY trunkid ASC');

        cachedExtensionsList = Array.isArray(extRows) ? extRows : [];
        cachedTrunksList = Array.isArray(trunkRows) ? trunkRows.map(r => ({
            key: r.channelid || r.name,
            name: r.name,
            tech: r.tech
        })) : [];

        knownExtensions.clear();
        cachedExtensionsList.forEach(r => {
            if (r.extension) knownExtensions.add(String(r.extension).trim());
        });

        knownTrunks.clear();
        Object.keys(trunkCanonicalMap).forEach(k => delete trunkCanonicalMap[k]);
        cachedTrunksList.forEach(r => {
            const key = (r.key || r.name || '').trim();
            if (key) {
                knownTrunks.add(key);
                trunkCanonicalMap[key.toLowerCase()] = key;
            }
            if (r.name) {
                const nm = r.name.trim();
                trunkCanonicalMap[nm.toLowerCase()] = key || nm;
            }
        });
    } catch (err) {
        if (!String(err.message || '').includes('Too many connections')) {
            console.error('Error refreshing peer registry:', err.message);
        }
    }
}
refreshPeerRegistry();
setInterval(refreshPeerRegistry, 60000);

function isTrunkPeer(peer) {
    if (!peer) return false;
    return knownTrunks.has(peer) || Boolean(trunkCanonicalMap[peer.toLowerCase()]);
}

function isExtensionPeer(peer) {
    if (!peer) return false;
    return knownExtensions.has(String(peer).trim());
}

function canonicalPeerName(rawPeer) {
    if (!rawPeer) return null;
    const trimmed = String(rawPeer).trim();
    if (trunkCanonicalMap[trimmed.toLowerCase()]) {
        return trunkCanonicalMap[trimmed.toLowerCase()];
    }
    return trimmed;
}

// Extract peer/extension name from channel string
function extractPeer(channel) {
    if (!channel) return null;
    const match = String(channel).match(/^(?:PJSIP|SIP|IAX2)\/(.+?)(?:-[0-9a-fA-F]+|@.*)?$/i);
    if (!match) return null;
    return canonicalPeerName(match[1]);
}

// Find correlated channels sharing the same linkedid or bridgeid
function getCorrelatedChannels(linkedid, bridgeid = '') {
    return Object.values(channelMap).filter(c =>
        (linkedid && c.linkedid === linkedid) ||
        (bridgeid && c.bridgeid && c.bridgeid === bridgeid)
    );
}

// Apply explicit internal extension-to-extension IN/OUT details when both caller & callee are extensions
function applyInternalExtCall(callerExt, calleeExt, linkedid = '', bridgeid = '', stateDesc = '') {
    const src = callerExt ? String(callerExt).trim() : '';
    const dst = calleeExt ? String(calleeExt).trim() : '';

    if (src && isExtensionPeer(src) && dst && isExtensionPeer(dst) && src !== dst) {
        callDetails[src] = { dir: 'OUT', number: dst, linkedid, bridgeid };
        callDetails[dst] = { dir: 'IN', number: src, linkedid, bridgeid };

        if (stateDesc === 'Up') {
            peerStatuses[src] = 'INCALL';
            peerStatuses[dst] = 'INCALL';
            if (!callStartTimes[src]) callStartTimes[src] = Date.now();
            if (!callStartTimes[dst]) callStartTimes[dst] = Date.now();
        } else {
            if (peerStatuses[src] !== 'INCALL') peerStatuses[src] = 'RINGING';
            if (peerStatuses[dst] !== 'INCALL') peerStatuses[dst] = 'RINGING';
        }

        emitPeerUpdate(src);
        emitPeerUpdate(dst);
        return true;
    }
    return false;
}

// Find extension associated with a given linkedid / bridgeid
function findLinkedExtension(linkedid, bridgeid = '', excludeExt = null) {
    const legs = getCorrelatedChannels(linkedid, bridgeid);
    for (const leg of legs) {
        if (leg.peer && !leg.isTrunk && isExtensionPeer(leg.peer) && leg.peer !== excludeExt) {
            return leg.peer;
        }
    }
    for (const [p, det] of Object.entries(callDetails)) {
        if (!isTrunkPeer(p) && isExtensionPeer(p) && p !== excludeExt && det) {
            if ((linkedid && det.linkedid === linkedid) || (bridgeid && det.bridgeid === bridgeid)) {
                return p;
            }
        }
    }
    return '';
}

// Find best real caller/dialed number across correlated channels
function resolveLinkedNumber(linkedid, excludePeer, preferDialed = false, bridgeid = '') {
    const legs = getCorrelatedChannels(linkedid, bridgeid);
    if (preferDialed) {
        for (const c of legs) {
            if (isValidNumber(c.exten, excludePeer) && c.exten.length >= 3) return c.exten;
        }
    }
    const trunkLeg = legs.find(c => c.isTrunk);
    if (trunkLeg) {
        if (isValidNumber(trunkLeg.callerNum, excludePeer)) return trunkLeg.callerNum;
        if (isValidNumber(trunkLeg.exten, excludePeer)) return trunkLeg.exten;
    }
    for (const c of legs) {
        if (isValidNumber(c.callerNum, excludePeer)) return c.callerNum;
    }
    for (const c of legs) {
        if (isValidNumber(c.exten, excludePeer)) return c.exten;
    }
    return '';
}

// Rebuild trunk status & sorted calls array from trunkActiveCalls map
function syncTrunkFromMap(trunkPeer) {
    if (!trunkPeer || !isTrunkPeer(trunkPeer)) return;
    const map = trunkActiveCalls[trunkPeer] || {};
    const callsList = Object.values(map).sort((a, b) => (a.startTime || 0) - (b.startTime || 0));

    callsList.forEach(c => {
        const linkedExt = findLinkedExtension(c.linkedid, c.bridgeid || '');
        if (linkedExt) c.exten = linkedExt;
    });

    if (callsList.length === 0) {
        peerStatuses[trunkPeer] = 'IDLE';
        delete callStartTimes[trunkPeer];
        delete callDetails[trunkPeer];
        delete trunkActiveCalls[trunkPeer];
        return;
    }

    const hasInCall = callsList.some(c => c.status === 'INCALL');
    peerStatuses[trunkPeer] = hasInCall ? 'INCALL' : 'RINGING';

    const earliestStart = callsList.reduce((min, c) => (!min || (c.startTime && c.startTime < min)) ? c.startTime : min, null);
    if (earliestStart) {
        callStartTimes[trunkPeer] = earliestStart;
    }

    const primary = callsList[callsList.length - 1];
    callDetails[trunkPeer] = {
        dir: primary.dir || 'IN',
        number: primary.number || '',
        exten: primary.exten || '',
        linkedid: primary.linkedid || '',
        bridgeid: primary.bridgeid || '',
        calls: callsList
    };
}

// Upsert or update a call entry inside a trunk's active calls map
function upsertTrunkCall(trunkPeer, callKey, patch) {
    if (!trunkPeer || !isTrunkPeer(trunkPeer) || !callKey) return;
    if (!trunkActiveCalls[trunkPeer]) trunkActiveCalls[trunkPeer] = {};
    const existing = trunkActiveCalls[trunkPeer][callKey] || {
        callKey,
        uniqueid: callKey,
        linkedid: callKey,
        bridgeid: '',
        dir: 'IN',
        number: '',
        exten: '',
        status: 'RINGING',
        startTime: Date.now()
    };

    const merged = { ...existing, ...patch };
    if (!isValidNumber(merged.number, trunkPeer) && isValidNumber(existing.number, trunkPeer)) {
        merged.number = existing.number;
    }
    if (!merged.exten && existing.exten) {
        merged.exten = existing.exten;
    }
    if (!merged.startTime) {
        merged.startTime = Date.now();
    }

    trunkActiveCalls[trunkPeer][callKey] = merged;
    syncTrunkFromMap(trunkPeer);
}

// Remove a call entry from a trunk when channel/call hangs up
function removeTrunkCall(trunkPeer, uid, linkedid = '') {
    if (!trunkPeer || !trunkActiveCalls[trunkPeer]) return;
    const map = trunkActiveCalls[trunkPeer];
    Object.keys(map).forEach(k => {
        const item = map[k];
        if (k === uid || item.uniqueid === uid || (linkedid && item.linkedid === linkedid && !channelMap[item.uniqueid])) {
            delete map[k];
        }
    });
    syncTrunkFromMap(trunkPeer);
}

// Fetch Today's Processed Calls, Total Processed Calls & Per-Extension Daily Calls from asteriskcdrdb.cdr pool
async function refreshCdrStats() {
    try {
        const [rows] = await cdrDbPool.execute(`
            SELECT 
                COUNT(DISTINCT uniqueid) AS total_calls,
                COUNT(DISTINCT CASE WHEN DATE(calldate) = CURDATE() THEN uniqueid END) AS today_calls
            FROM cdr
        `);

        const [todayRows] = await cdrDbPool.execute(`
            SELECT uniqueid, src, dst, channel, dstchannel
            FROM cdr
            WHERE DATE(calldate) = CURDATE()
        `);

        const extCallSets = {};
        if (Array.isArray(todayRows)) {
            todayRows.forEach(r => {
                const uid = r.uniqueid;
                const matchedExts = new Set();

                const src = String(r.src || '').trim();
                const dst = String(r.dst || '').trim();
                if (knownExtensions.has(src)) matchedExts.add(src);
                if (knownExtensions.has(dst)) matchedExts.add(dst);

                [r.channel, r.dstchannel].forEach(ch => {
                    const p = extractPeer(ch);
                    if (p && knownExtensions.has(p)) {
                        matchedExts.add(p);
                    }
                });

                matchedExts.forEach(ext => {
                    if (!extCallSets[ext]) extCallSets[ext] = new Set();
                    extCallSets[ext].add(uid);
                });
            });
        }

        const extDailyCalls = {};
        Object.keys(extCallSets).forEach(ext => {
            extDailyCalls[ext] = extCallSets[ext].size;
        });

        if (rows && rows.length > 0) {
            cachedCdrStats = {
                totalProcessed: Number(rows[0].total_calls) || 0,
                todayProcessed: Number(rows[0].today_calls) || 0,
                extDailyCalls
            };
            io.emit('call_summary', getCallSummary());
        }
    } catch (err) {
        if (!String(err.message || '').includes('Too many connections')) {
            console.error('Error fetching CDR call stats:', err.message);
        }
    }
}
setTimeout(refreshCdrStats, 2000);
setInterval(refreshCdrStats, 20000);

// Fetch Public IP and Location metadata periodically (IPv4 forced with fast timeout)
function refreshPublicIpAndLocation() {
    https.get('https://ipinfo.io/json', { timeout: 2500, family: 4 }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
            try {
                const parsed = JSON.parse(data);
                if (parsed.ip) cachedPublicIp = parsed.ip;
                if (parsed.city && parsed.country) {
                    cachedLocation = `${parsed.city}, ${parsed.country}`;
                } else if (parsed.timezone) {
                    cachedLocation = parsed.timezone;
                }
            } catch (e) {}
        });
    }).on('error', () => {
        https.get('https://api.ipify.org?format=json', { timeout: 2500, family: 4 }, (res2) => {
            let data2 = '';
            res2.on('data', chunk => data2 += chunk);
            res2.on('end', () => {
                try {
                    const p2 = JSON.parse(data2);
                    if (p2.ip) cachedPublicIp = p2.ip;
                } catch (e) {}
            });
        }).on('error', () => {
            if (cachedPublicIp === 'Detecting...') cachedPublicIp = 'Unavailable';
        });
    });
}
refreshPublicIpAndLocation();
setInterval(refreshPublicIpAndLocation, 300000);

// Get local primary IPv4 address
function getLocalServerIp() {
    const interfaces = os.networkInterfaces();
    for (const name of Object.keys(interfaces)) {
        for (const iface of interfaces[name]) {
            if (iface.family === 'IPv4' && !iface.internal) {
                return iface.address;
            }
        }
    }
    return '127.0.0.1';
}

// Calculate real-time CPU usage percentage
function getCpuUsagePercent() {
    const currCpuInfo = os.cpus();
    let idleDiff = 0;
    let totalDiff = 0;

    for (let i = 0; i < currCpuInfo.length; i++) {
        const prev = prevCpuInfo[i].times;
        const curr = currCpuInfo[i].times;
        const prevTotal = prev.user + prev.nice + prev.sys + prev.idle + prev.irq;
        const currTotal = curr.user + curr.nice + curr.sys + curr.idle + curr.irq;
        idleDiff += (curr.idle - prev.idle);
        totalDiff += (currTotal - prevTotal);
    }
    prevCpuInfo = currCpuInfo;
    if (totalDiff === 0) return 0;
    return Math.max(0, Math.min(100, Math.round(((totalDiff - idleDiff) / totalDiff) * 100)));
}

// Broadcast System Telemetry (CPU, RAM, Storage, IPs, Location)
function broadcastSystemStats() {
    const cpuPercent = getCpuUsagePercent();
    const cpuCores = os.cpus().length;

    const totalMemBytes = os.totalmem();
    const freeMemBytes = os.freemem();
    const usedMemBytes = totalMemBytes - freeMemBytes;
    const ramPercent = Math.round((usedMemBytes / totalMemBytes) * 100);
    const usedMemGB = (usedMemBytes / (1024 ** 3)).toFixed(2);
    const totalMemGB = (totalMemBytes / (1024 ** 3)).toFixed(2);

    exec("df -BG / | tail -1 | awk '{print $2,$3,$4,$5}'", (err, stdout) => {
        let diskTotal = '0G', diskUsed = '0G', diskFree = '0G', diskPercent = '0%';
        if (!err && stdout) {
            const parts = stdout.trim().split(/\s+/);
            if (parts.length >= 4) {
                diskTotal = parts[0];
                diskUsed = parts[1];
                diskFree = parts[2];
                diskPercent = parts[3];
            }
        }

        io.emit('system_stats', {
            cpuPercent,
            cpuCores,
            ramPercent,
            usedMemGB,
            totalMemGB,
            diskTotal,
            diskUsed,
            diskFree,
            diskPercent,
            serverIp: getLocalServerIp(),
            publicIp: cachedPublicIp,
            location: cachedLocation,
            serverTimestamp: Date.now()
        });
    });
}
setInterval(broadcastSystemStats, 2000);

// Compute live active call count + CDR processed counts + per-extension daily counts
function getCallSummary() {
    let activeCalls = 0;
    const activeChannels = Object.values(channelMap);

    if (activeChannels.length > 0) {
        const seenBridges = new Set();
        activeChannels.forEach(ch => {
            const key = ch.linkedid || ch.uniqueid;
            if (!seenBridges.has(key)) {
                seenBridges.add(key);
                activeCalls++;
            }
        });
    } else {
        Object.values(peerStatuses).forEach(st => {
            if (st === 'INCALL' || st === 'RINGING') activeCalls++;
        });
    }

    return {
        totalActive: activeCalls,
        todayProcessed: cachedCdrStats.todayProcessed,
        totalProcessed: cachedCdrStats.totalProcessed,
        extDailyCalls: cachedCdrStats.extDailyCalls
    };
}

// Broadcast updated extension/trunk call state + summary counts
function emitPeerUpdate(peer) {
    if (!peer) return;
    if (isTrunkPeer(peer)) {
        syncTrunkFromMap(peer);
    }
    const status = peerStatuses[peer] || 'IDLE';
    const startTime = callStartTimes[peer] || null;
    const callInfo = callDetails[peer] || null;
    io.emit('ext_status', {
        exten: peer,
        status,
        startTime,
        callInfo,
        callSummary: getCallSummary()
    });
}

// Notify all trunks linked to a specific extension so their linked extension label updates live
function refreshTrunksForExtension(extPeer) {
    Object.keys(trunkActiveCalls).forEach(trPeer => {
        const calls = Object.values(trunkActiveCalls[trPeer] || {});
        if (calls.some(c => c.exten === extPeer || (!c.exten && channelMap[c.uniqueid]))) {
            emitPeerUpdate(trPeer);
        }
    });
}

// Parse Asterisk CLI outputs for SIP and PJSIP registration & IP
function parseRegistrationData(sipOutput, pjsipEndpoints, pjsipContacts) {
    const currentRegs = {};
    const now = Date.now();

    if (sipOutput) {
        sipOutput.split('\n').forEach(line => {
            const match = line.match(/^(.+?)(?:\/[^\s]+)?\s+(\d+\.\d+\.\d+\.\d+|\(Unspecified\))\s+.*?(OK|Unmonitored)/i);
            if (match) {
                const peer = canonicalPeerName(match[1]);
                const ip = match[2] === '(Unspecified)' ? '' : match[2];
                const isOk = /OK/i.test(match[3]) && ip !== '';
                currentRegs[peer] = { registered: isOk, ip };
            }
        });
    }

    const contactIpMap = {};
    if (pjsipContacts) {
        pjsipContacts.split('\n').forEach(line => {
            const match = line.match(/Contact:\s+([^\/\s]+)\/sip:[^@]+@(\d+\.\d+\.\d+\.\d+)/i) ||
                          line.match(/Contact:\s+([^\/\s]+)\/sip:(\d+\.\d+\.\d+\.\d+)/i);
            if (match) {
                contactIpMap[canonicalPeerName(match[1])] = match[2];
            }
        });
    }

    if (pjsipEndpoints) {
        pjsipEndpoints.split('\n').forEach(line => {
            const match = line.match(/Endpoint:\s+(.+?)(?:\/\S+)?\s+(Not in use|In use|Ringing|Busy|Avail|Unavailable)/i);
            if (match) {
                const peer = canonicalPeerName(match[1]);
                const state = match[2];
                const isReg = !/Unavailable/i.test(state);
                const ip = contactIpMap[peer] || '';
                if (!currentRegs[peer] || isReg) {
                    currentRegs[peer] = { registered: isReg, ip };
                }
            }
        });
    }

    Object.keys(currentRegs).forEach(peer => {
        const prev = regStatus[peer];
        const curr = currentRegs[peer];
        if (curr.registered) {
            curr.regSince = (prev && prev.registered && prev.regSince) ? prev.regSince : now;
        } else {
            curr.regSince = null;
        }
        regStatus[peer] = curr;
        io.emit('reg_status', { peer, ...curr });
    });
}

function pollRegistrations() {
    let sipOut = '', pjsipEpOut = '', pjsipContactOut = '';
    ami.action({ action: 'Command', command: 'sip show peers' }, (err, res) => {
        if (!err && res) sipOut = normalizeAmiOut(res);
        ami.action({ action: 'Command', command: 'pjsip show endpoints' }, (err2, res2) => {
            if (!err2 && res2) pjsipEpOut = normalizeAmiOut(res2);
            ami.action({ action: 'Command', command: 'pjsip show contacts' }, (err3, res3) => {
                if (!err3 && res3) pjsipContactOut = normalizeAmiOut(res3);
                parseRegistrationData(sipOut, pjsipEpOut, pjsipContactOut);
            });
        });
    });
}

// Reconcile active channels to prevent stale states & handle internal calls + trunk calls
function syncActiveChannels() {
    ami.action({ action: 'Command', command: 'core show channels concise' }, (err, res) => {
        if (err || !res) return;
        const output = normalizeAmiOut(res);
        const activePeers = new Set();
        const activeUniqueIds = new Set();
        const seenTrunkCallKeysByPeer = {};

        const parsedRows = [];
        output.split('\n').forEach(line => {
            if (!line.includes('!')) return;
            const parts = line.split('!');
            parsedRows.push({
                channel: parts[0] || '',
                context: (parts[1] || '').toLowerCase(),
                exten: parts[2] || '',
                state: parts[4] || '',
                app: parts[5] || '',
                appData: parts[6] || '',
                callerNum: parts[7] || '',
                uniqueid: parts[11] || parts[0] || '',
                connectedNum: parts[12] || parts[2] || '',
                bridgeId: parts[13] || ''
            });
        });

        const bridgeOutInfo = {};
        parsedRows.forEach(r => {
            const p = extractPeer(r.channel);
            const dialedFromApp = extractDialedNumberFromData(r.appData);
            if (p && !isTrunkPeer(p) && (isOutgoingContext(r.context, r.channel) || dialedFromApp)) {
                const num = dialedFromApp || (isValidNumber(r.exten, p) ? r.exten : '');
                if (r.bridgeId) {
                    bridgeOutInfo[r.bridgeId] = { exten: p, number: num };
                }
            }
        });

        parsedRows.forEach(r => {
            const { channel, context, exten, state, appData, callerNum, uniqueid, connectedNum, bridgeId } = r;
            const peer = extractPeer(channel);
            activeUniqueIds.add(uniqueid);
            if (channelMap[uniqueid] && bridgeId) {
                channelMap[uniqueid].bridgeid = bridgeId;
            }

            if (peer) {
                activePeers.add(peer);
                const trunkFlag = isTrunkPeer(peer);
                const mappedSt = state === 'Up' ? 'INCALL' : ((state === 'Ring' || state === 'Ringing') ? 'RINGING' : 'INCALL');
                const dialedFromApp = extractDialedNumberFromData(appData);
                const isOutAppLine = String(appData || '').toLowerCase().includes('outgoing line');
                const outMeta = bridgeId ? bridgeOutInfo[bridgeId] : null;
                const linkedid = (trunkActiveCalls[peer] && trunkActiveCalls[peer][uniqueid] && trunkActiveCalls[peer][uniqueid].linkedid)
                    || ((channelMap[uniqueid] && channelMap[uniqueid].linkedid) || uniqueid);

                if (trunkFlag) {
                    const existingEntry = (trunkActiveCalls[peer] && trunkActiveCalls[peer][uniqueid]) || null;
                    const isOut = Boolean(
                        isOutgoingContext(context, channel) ||
                        isOutAppLine ||
                        outMeta ||
                        (existingEntry && existingEntry.dir === 'OUT')
                    );

                    const remote = isOut
                        ? ((outMeta && isValidNumber(outMeta.number, peer))
                            ? outMeta.number
                            : (isValidNumber(callerNum, peer) ? callerNum : (isValidNumber(exten, peer) ? exten : (existingEntry ? existingEntry.number : ''))))
                        : (isValidNumber(callerNum, peer) ? callerNum : (isValidNumber(connectedNum, peer) ? connectedNum : (existingEntry ? existingEntry.number : '')));

                    const linkedExt = (outMeta && outMeta.exten) || findLinkedExtension(linkedid, bridgeId) || (existingEntry ? existingEntry.exten : '');

                    if (!seenTrunkCallKeysByPeer[peer]) seenTrunkCallKeysByPeer[peer] = new Set();
                    seenTrunkCallKeysByPeer[peer].add(uniqueid);

                    upsertTrunkCall(peer, uniqueid, {
                        uniqueid,
                        linkedid,
                        bridgeid: bridgeId || (existingEntry ? existingEntry.bridgeid : ''),
                        dir: isOut ? 'OUT' : 'IN',
                        number: remote,
                        exten: linkedExt,
                        status: mappedSt,
                        startTime: existingEntry ? existingEntry.startTime : Date.now()
                    });
                    emitPeerUpdate(peer);
                } else {
                    if (state === 'Up' || state === 'Ring' || state === 'Ringing') {
                        if (state === 'Up') {
                            peerStatuses[peer] = 'INCALL';
                            if (!callStartTimes[peer]) callStartTimes[peer] = Date.now();
                        } else if (peerStatuses[peer] !== 'INCALL') {
                            peerStatuses[peer] = 'RINGING';
                        }

                        const outNum = dialedFromApp || (isValidNumber(exten, peer) ? exten : ((outMeta && outMeta.number) || ''));
                        const inNum = isValidNumber(callerNum, peer) ? callerNum : (isValidNumber(connectedNum, peer) ? connectedNum : '');

                        if (isExtensionPeer(peer) && isValidNumber(outNum, peer) && isExtensionPeer(outNum)) {
                            applyInternalExtCall(peer, outNum, linkedid, bridgeId, state);
                        } else if (isExtensionPeer(peer) && isValidNumber(inNum, peer) && isExtensionPeer(inNum)) {
                            applyInternalExtCall(inNum, peer, linkedid, bridgeId, state);
                        } else {
                            const isOutExt = Boolean(
                                isOutgoingContext(context, channel) ||
                                dialedFromApp ||
                                (context.includes('from-internal') && isValidNumber(exten, peer))
                            );
                            if (isOutExt && isValidNumber(outNum, peer)) {
                                callDetails[peer] = {
                                    dir: 'OUT',
                                    number: outNum,
                                    linkedid,
                                    bridgeid: bridgeId
                                };
                            } else if (!callDetails[peer] && isValidNumber(inNum, peer)) {
                                callDetails[peer] = {
                                    dir: 'IN',
                                    number: inNum,
                                    linkedid,
                                    bridgeid: bridgeId
                                };
                            }
                        }
                        emitPeerUpdate(peer);
                        refreshTrunksForExtension(peer);
                    }
                }
            }
        });

        Object.keys(trunkActiveCalls).forEach(trPeer => {
            const validSet = seenTrunkCallKeysByPeer[trPeer] || new Set();
            let changed = false;
            Object.keys(trunkActiveCalls[trPeer]).forEach(ck => {
                if (!validSet.has(ck) && !activeUniqueIds.has(ck)) {
                    delete trunkActiveCalls[trPeer][ck];
                    changed = true;
                }
            });
            if (changed) {
                syncTrunkFromMap(trPeer);
                emitPeerUpdate(trPeer);
            }
        });

        Object.keys(channelMap).forEach(uid => {
            if (!activeUniqueIds.has(uid) && !activePeers.has(channelMap[uid].peer)) {
                delete channelMap[uid];
            }
        });

        Object.keys(peerStatuses).forEach(peer => {
            if (!isTrunkPeer(peer) && peerStatuses[peer] !== 'IDLE' && !activePeers.has(peer)) {
                peerStatuses[peer] = 'IDLE';
                delete callStartTimes[peer];
                delete callDetails[peer];
                emitPeerUpdate(peer);
            }
        });

        io.emit('call_summary', getCallSummary());
    });
}

setInterval(pollRegistrations, 6000);
setInterval(syncActiveChannels, 5000);
setTimeout(() => {
    pollRegistrations();
    syncActiveChannels();
    broadcastSystemStats();
}, 1500);

// Listen to Asterisk AMI Real-Time Events
ami.on('managerevent', (evt) => {
    const eventName = evt.event;

    if (eventName === 'Newchannel') {
        const peer = extractPeer(evt.channel);
        const callerNum = evt.calleridnum || '';
        const dialedExten = evt.exten || '';
        const uid = evt.uniqueid;
        const linkedid = evt.linkedid || uid;
        const context = (evt.context || '').toLowerCase();
        const trunkFlag = isTrunkPeer(peer);

        if (uid) {
            const existing = channelMap[uid] || {};
            channelMap[uid] = {
                uniqueid: uid,
                linkedid,
                bridgeid: existing.bridgeid || '',
                peer,
                channel: evt.channel,
                callerNum: isValidNumber(callerNum) ? callerNum : (existing.callerNum || ''),
                exten: isValidNumber(dialedExten) ? dialedExten : (existing.exten || ''),
                context,
                isTrunk: trunkFlag,
                state: evt.channelstatedesc || 'Ring'
            };
        }

        if (peer) {
            const linked = getCorrelatedChannels(linkedid);
            const originatingExtLeg = linked.find(c => !c.isTrunk && c.peer && isOutgoingContext(c.context, c.channel));
            const incomingTrunkLeg = linked.find(c => c.isTrunk && !isOutgoingContext(c.context, c.channel) && (c.context.includes('from-trunk') || c.context.includes('from-pstn') || c.context.includes('from-did')));

            if (trunkFlag) {
                const isOut = Boolean(originatingExtLeg || isOutgoingContext(context, evt.channel));
                const num = isOut
                    ? ((originatingExtLeg && isValidNumber(originatingExtLeg.exten, peer))
                        ? originatingExtLeg.exten
                        : (isValidNumber(dialedExten, peer) ? dialedExten : (isValidNumber(callerNum, peer) ? callerNum : resolveLinkedNumber(linkedid, peer, true))))
                    : (isValidNumber(callerNum, peer) ? callerNum : (isValidNumber(dialedExten, peer) ? dialedExten : resolveLinkedNumber(linkedid, peer, false)));

                const linkedExt = (originatingExtLeg && originatingExtLeg.peer) || findLinkedExtension(linkedid);
                upsertTrunkCall(peer, uid || linkedid, {
                    uniqueid: uid || linkedid,
                    linkedid,
                    dir: isOut ? 'OUT' : 'IN',
                    number: isValidNumber(num, peer) ? num : '',
                    exten: linkedExt,
                    status: evt.channelstatedesc === 'Up' ? 'INCALL' : 'RINGING',
                    startTime: Date.now()
                });
                emitPeerUpdate(peer);
            } else {
                const otherLeg = linked.find(c => !c.isTrunk && c.peer && isExtensionPeer(c.peer) && c.peer !== peer);
                if (isExtensionPeer(peer) && isValidNumber(dialedExten, peer) && isExtensionPeer(dialedExten)) {
                    applyInternalExtCall(peer, dialedExten, linkedid, '', evt.channelstatedesc);
                } else if (isExtensionPeer(peer) && isValidNumber(callerNum, peer) && isExtensionPeer(callerNum)) {
                    applyInternalExtCall(callerNum, peer, linkedid, '', evt.channelstatedesc);
                } else if (otherLeg && isExtensionPeer(peer)) {
                    if (isOutgoingContext(context, evt.channel) || callerNum === peer) {
                        applyInternalExtCall(peer, otherLeg.peer, linkedid, '', evt.channelstatedesc);
                    } else {
                        applyInternalExtCall(otherLeg.peer, peer, linkedid, '', evt.channelstatedesc);
                    }
                } else if (incomingTrunkLeg || context.includes('ext-queues') || context.includes('ext-group') || context.includes('from-trunk') || context.includes('from-pstn')) {
                    const externalCaller = resolveLinkedNumber(linkedid, peer, false) || (isValidNumber(callerNum, peer) ? callerNum : '');
                    if (isValidNumber(externalCaller, peer)) {
                        callDetails[peer] = { dir: 'IN', number: externalCaller, linkedid };
                    }
                } else if (isOutgoingContext(context, evt.channel) && isValidNumber(dialedExten, peer)) {
                    callDetails[peer] = { dir: 'OUT', number: dialedExten, linkedid };
                } else if (callerNum === peer && isValidNumber(dialedExten, peer)) {
                    callDetails[peer] = { dir: 'OUT', number: dialedExten, linkedid };
                } else if (isValidNumber(callerNum, peer)) {
                    callDetails[peer] = { dir: 'IN', number: callerNum, linkedid };
                } else {
                    const fallbackNum = resolveLinkedNumber(linkedid, peer, false);
                    if (isValidNumber(fallbackNum, peer)) callDetails[peer] = { dir: 'IN', number: fallbackNum, linkedid };
                }

                if (peerStatuses[peer] !== 'INCALL') {
                    peerStatuses[peer] = 'RINGING';
                }
                emitPeerUpdate(peer);
                refreshTrunksForExtension(peer);
            }
        }
    }

    if (eventName === 'DialBegin') {
        const srcPeer = extractPeer(evt.channel);
        const destPeer = extractPeer(evt.destchannel);
        const srcCaller = evt.calleridnum || '';
        const linkedid = evt.linkedid || evt.uniqueid;
        const destUid = evt.destuniqueid || evt.uniqueid;
        const ctx = (evt.context || '').toLowerCase();
        const linked = getCorrelatedChannels(linkedid);
        const hasIncomingTrunk = linked.some(c => c.isTrunk && !isOutgoingContext(c.context, c.channel) && (c.context.includes('from-trunk') || c.context.includes('from-pstn') || c.context.includes('from-did')));

        if (srcPeer && !isTrunkPeer(srcPeer) && destPeer && !isTrunkPeer(destPeer) && isExtensionPeer(srcPeer) && isExtensionPeer(destPeer)) {
            applyInternalExtCall(srcPeer, destPeer, linkedid, '', 'Ring');
            return;
        }

        if (srcPeer && !isTrunkPeer(srcPeer) && destPeer && isTrunkPeer(destPeer)) {
            const rawTarget = extractDialedNumberFromData(evt.dialstring)
                || (isValidNumber(evt.destcalleridnum, srcPeer) ? evt.destcalleridnum : '')
                || (isValidNumber(evt.exten, srcPeer) ? evt.exten : '')
                || resolveLinkedNumber(linkedid, srcPeer, true);
            const cleanTarget = String(rawTarget || '').replace(/^.*\/([^@]+).*$/, '$1');

            if (isValidNumber(cleanTarget, srcPeer)) {
                callDetails[srcPeer] = { dir: 'OUT', number: cleanTarget, linkedid };
                if (peerStatuses[srcPeer] !== 'INCALL') peerStatuses[srcPeer] = 'RINGING';
                emitPeerUpdate(srcPeer);
            }

            upsertTrunkCall(destPeer, destUid, {
                uniqueid: destUid,
                linkedid,
                dir: 'OUT',
                number: isValidNumber(cleanTarget, destPeer) ? cleanTarget : '',
                exten: srcPeer,
                status: 'RINGING',
                startTime: Date.now()
            });
            emitPeerUpdate(destPeer);
            return;
        }

        if (srcPeer && !isTrunkPeer(srcPeer)) {
            if (hasIncomingTrunk) {
                const extCaller = resolveLinkedNumber(linkedid, srcPeer, false) || (isValidNumber(srcCaller, srcPeer) ? srcCaller : '');
                if (isValidNumber(extCaller, srcPeer)) {
                    callDetails[srcPeer] = { dir: 'IN', number: extCaller, linkedid };
                    emitPeerUpdate(srcPeer);
                }
            } else {
                const rawDest = extractDialedNumberFromData(evt.dialstring)
                    || (isValidNumber(evt.destcalleridnum, srcPeer) ? evt.destcalleridnum : '')
                    || (isValidNumber(evt.exten, srcPeer) ? evt.exten : '')
                    || (isValidNumber(destPeer, srcPeer) ? destPeer : resolveLinkedNumber(linkedid, srcPeer, true));
                const cleanDest = String(rawDest || '').replace(/^.*\/([^@]+).*$/, '$1');
                if (isValidNumber(cleanDest, srcPeer)) {
                    if (isExtensionPeer(srcPeer) && isExtensionPeer(cleanDest)) {
                        applyInternalExtCall(srcPeer, cleanDest, linkedid, '', 'Ring');
                    } else {
                        const dir = isOutgoingContext(ctx, evt.channel) || isExtensionPeer(srcPeer) ? 'OUT' : 'IN';
                        callDetails[srcPeer] = { dir, number: cleanDest, linkedid };
                        emitPeerUpdate(srcPeer);
                    }
                }
            }
        }

        if (destPeer) {
            if (isTrunkPeer(destPeer)) {
                const srcIsExt = srcPeer && !isTrunkPeer(srcPeer);
                const dir = (srcIsExt || isOutgoingContext(ctx, evt.channel)) ? 'OUT' : 'IN';
                const dialedFromDialStr = extractDialedNumberFromData(evt.dialstring)
                    || (isValidNumber(evt.exten, srcPeer) ? evt.exten : '')
                    || (isValidNumber(evt.destcalleridnum, destPeer) ? evt.destcalleridnum : '');
                const dialedNumber = isValidNumber(dialedFromDialStr, destPeer)
                    ? dialedFromDialStr
                    : ((callDetails[srcPeer] && isValidNumber(callDetails[srcPeer].number, destPeer))
                        ? callDetails[srcPeer].number
                        : resolveLinkedNumber(linkedid, destPeer, dir === 'OUT'));
                const extNum = srcIsExt ? srcPeer : findLinkedExtension(linkedid);

                upsertTrunkCall(destPeer, destUid, {
                    uniqueid: destUid,
                    linkedid,
                    dir,
                    number: isValidNumber(dialedNumber, destPeer) ? dialedNumber : '',
                    exten: extNum,
                    status: 'RINGING',
                    startTime: Date.now()
                });
                emitPeerUpdate(destPeer);
            } else {
                const incomingNum = resolveLinkedNumber(linkedid, destPeer, false)
                    || (isValidNumber(srcCaller, destPeer) ? srcCaller : (isValidNumber(srcPeer, destPeer) ? srcPeer : ''));
                if (isExtensionPeer(destPeer) && isValidNumber(incomingNum, destPeer) && isExtensionPeer(incomingNum)) {
                    applyInternalExtCall(incomingNum, destPeer, linkedid, '', 'Ring');
                } else {
                    if (isValidNumber(incomingNum, destPeer)) {
                        callDetails[destPeer] = { dir: 'IN', number: incomingNum, linkedid };
                    }
                    if (peerStatuses[destPeer] !== 'INCALL') {
                        peerStatuses[destPeer] = 'RINGING';
                    }
                    emitPeerUpdate(destPeer);
                    refreshTrunksForExtension(destPeer);
                }
            }
        }
    }

    if (eventName === 'Newstate') {
        const peer = extractPeer(evt.channel);
        const stateDesc = evt.channelstatedesc;
        const uid = evt.uniqueid;
        const linkedid = evt.linkedid || uid;
        const connectedNum = evt.connectedlinenum || '';
        const callerNum = evt.calleridnum || '';

        if (uid && channelMap[uid]) {
            channelMap[uid].state = stateDesc;
        }

        if (peer) {
            if (isTrunkPeer(peer)) {
                const st = stateDesc === 'Up' ? 'INCALL' : 'RINGING';
                const linkedExt = findLinkedExtension(linkedid);
                const existingCall = trunkActiveCalls[peer] && trunkActiveCalls[peer][uid];
                const isOut = Boolean(existingCall && existingCall.dir === 'OUT');
                const remote = resolveLinkedNumber(linkedid, peer, isOut)
                    || (isValidNumber(connectedNum, peer) ? connectedNum : (isValidNumber(callerNum, peer) ? callerNum : ''));

                upsertTrunkCall(peer, uid, {
                    uniqueid: uid,
                    linkedid,
                    number: isValidNumber(remote, peer) ? remote : '',
                    exten: linkedExt,
                    status: st,
                    startTime: existingCall ? existingCall.startTime : Date.now()
                });
                emitPeerUpdate(peer);
            } else {
                if (!callDetails[peer]) {
                    const remote = resolveLinkedNumber(linkedid, peer, false)
                        || (isValidNumber(connectedNum, peer) ? connectedNum : (isValidNumber(callerNum, peer) ? callerNum : ''));
                    if (isValidNumber(remote, peer)) {
                        if (isExtensionPeer(peer) && isExtensionPeer(remote)) {
                            const existingDet = callDetails[remote];
                            if (existingDet && existingDet.dir === 'OUT' && existingDet.number === peer) {
                                applyInternalExtCall(remote, peer, linkedid, '', stateDesc);
                            } else {
                                applyInternalExtCall(peer, remote, linkedid, '', stateDesc);
                            }
                        } else {
                            callDetails[peer] = { dir: 'IN', number: remote, linkedid };
                        }
                    }
                } else {
                    callDetails[peer].linkedid = linkedid;
                    if (isExtensionPeer(peer) && isExtensionPeer(callDetails[peer].number)) {
                        const otherExt = callDetails[peer].number;
                        if (callDetails[peer].dir === 'OUT') {
                            applyInternalExtCall(peer, otherExt, linkedid, '', stateDesc);
                        } else {
                            applyInternalExtCall(otherExt, peer, linkedid, '', stateDesc);
                        }
                    }
                }

                if (stateDesc === 'Up') {
                    peerStatuses[peer] = 'INCALL';
                    if (!callStartTimes[peer]) callStartTimes[peer] = Date.now();
                    emitPeerUpdate(peer);
                    refreshTrunksForExtension(peer);
                } else if (stateDesc === 'Ringing' || stateDesc === 'Ring') {
                    if (peerStatuses[peer] !== 'INCALL') {
                        peerStatuses[peer] = 'RINGING';
                        emitPeerUpdate(peer);
                    }
                }
            }
        }
    }

    if (eventName === 'BridgeEnter') {
        const peer = extractPeer(evt.channel);
        const uid = evt.uniqueid;
        const bridgeid = evt.bridgeuniqueid || '';
        const linkedid = evt.linkedid || (channelMap[uid] && channelMap[uid].linkedid) || uid;
        const connectedNum = evt.connectedlinenum || '';
        const callerNum = evt.calleridnum || '';

        if (uid && channelMap[uid] && bridgeid) {
            channelMap[uid].bridgeid = bridgeid;
        }

        if (peer) {
            if (isTrunkPeer(peer)) {
                const linkedExt = findLinkedExtension(linkedid, bridgeid);
                const existingCall = trunkActiveCalls[peer] && trunkActiveCalls[peer][uid];
                const isOut = Boolean(existingCall && existingCall.dir === 'OUT');
                const remote = resolveLinkedNumber(linkedid, peer, isOut, bridgeid)
                    || (isValidNumber(connectedNum, peer) ? connectedNum : (isValidNumber(callerNum, peer) ? callerNum : ''));

                upsertTrunkCall(peer, uid, {
                    uniqueid: uid,
                    linkedid,
                    bridgeid,
                    number: isValidNumber(remote, peer) ? remote : '',
                    exten: linkedExt,
                    status: 'INCALL',
                    startTime: existingCall ? existingCall.startTime : Date.now()
                });
                emitPeerUpdate(peer);
            } else {
                const remote = resolveLinkedNumber(linkedid, peer, false, bridgeid)
                    || (isValidNumber(connectedNum, peer) ? connectedNum : (isValidNumber(callerNum, peer) ? callerNum : ''));
                if (isExtensionPeer(peer) && isValidNumber(remote, peer) && isExtensionPeer(remote)) {
                    const existingDet = callDetails[remote];
                    if (existingDet && existingDet.dir === 'OUT' && existingDet.number === peer) {
                        applyInternalExtCall(remote, peer, linkedid, bridgeid, 'Up');
                    } else if (callDetails[peer] && callDetails[peer].dir === 'OUT' && callDetails[peer].number === remote) {
                        applyInternalExtCall(peer, remote, linkedid, bridgeid, 'Up');
                    } else {
                        applyInternalExtCall(remote, peer, linkedid, bridgeid, 'Up');
                    }
                } else {
                    peerStatuses[peer] = 'INCALL';
                    if (!callStartTimes[peer]) callStartTimes[peer] = Date.now();
                    if (!callDetails[peer]) {
                        if (isValidNumber(remote, peer)) {
                            callDetails[peer] = { dir: 'IN', number: remote, linkedid, bridgeid };
                        }
                    } else {
                        callDetails[peer].linkedid = linkedid;
                        callDetails[peer].bridgeid = bridgeid;
                    }
                    emitPeerUpdate(peer);
                    refreshTrunksForExtension(peer);
                }
            }
        }
    }

    if (eventName === 'Hangup') {
        const peer = extractPeer(evt.channel);
        const uid = evt.uniqueid;
        const linkedid = evt.linkedid || (channelMap[uid] && channelMap[uid].linkedid) || '';
        if (uid) delete channelMap[uid];

        if (peer) {
            if (isTrunkPeer(peer)) {
                removeTrunkCall(peer, uid, linkedid);
                emitPeerUpdate(peer);
            } else {
                peerStatuses[peer] = 'IDLE';
                delete callStartTimes[peer];
                delete callDetails[peer];
                emitPeerUpdate(peer);
                refreshTrunksForExtension(peer);
            }
        }
        setTimeout(refreshCdrStats, 2500);
    }

    if (eventName === 'PeerStatus') {
        const peer = extractPeer(evt.peer);
        if (peer) {
            const isReg = /Registered|Reachable/i.test(evt.peerstatus);
            const ip = evt.address ? evt.address.split(':')[0] : (regStatus[peer]?.ip || '');
            const prev = regStatus[peer];
            const regSince = isReg ? ((prev && prev.registered && prev.regSince) ? prev.regSince : Date.now()) : null;
            regStatus[peer] = { registered: isReg, ip, regSince };
            io.emit('reg_status', { peer, ...regStatus[peer] });
        }
    }
});

// REST API: Fetch extensions from MySQL pool (with cached fallback if DB busy)
app.get('/api/extensions', async (req, res) => {
    try {
        const [rows] = await dbPool.execute('SELECT extension, name FROM users ORDER BY CAST(extension AS UNSIGNED) ASC');
        cachedExtensionsList = Array.isArray(rows) ? rows : [];
        res.json(cachedExtensionsList);
    } catch (err) {
        if (cachedExtensionsList.length > 0) {
            return res.json(cachedExtensionsList);
        }
        res.json([]);
    }
});

// REST API: Fetch trunks from MySQL pool (with cached fallback if DB busy)
app.get('/api/trunks', async (req, res) => {
    try {
        const [rows] = await dbPool.execute('SELECT trunkid, name, tech, channelid FROM trunks WHERE disabled = "off" ORDER BY trunkid ASC');
        const formatted = Array.isArray(rows) ? rows.map(r => ({
            key: r.channelid || r.name,
            name: r.name,
            tech: r.tech
        })) : [];
        cachedTrunksList = formatted;
        res.json(formatted);
    } catch (err) {
        if (cachedTrunksList.length > 0) {
            return res.json(cachedTrunksList);
        }
        res.json([]);
    }
});

// Socket.io Client Connections & Operator Actions
io.on('connection', (socket) => {
    socket.emit('initial_state', {
        statuses: peerStatuses,
        startTimes: callStartTimes,
        callDetails: callDetails,
        registrations: regStatus,
        callSummary: getCallSummary()
    });
    broadcastSystemStats();

    // Reset & clear hung call/session ONLY for the targeted extension
    socket.on('action_reset', ({ exten }) => {
        const targetExt = String(exten || '').trim();
        if (!targetExt) return;

        peerStatuses[targetExt] = 'IDLE';
        delete callStartTimes[targetExt];
        delete callDetails[targetExt];

        Object.keys(channelMap).forEach(uid => {
            if (channelMap[uid] && channelMap[uid].peer === targetExt) {
                delete channelMap[uid];
            }
        });

        ami.action({ action: 'Command', command: 'core show channels concise' }, (err, res) => {
            if (!err && res) {
                const output = normalizeAmiOut(res);
                output.split('\n').forEach(line => {
                    if (!line.includes('!')) return;
                    const channel = line.split('!')[0];
                    if (extractPeer(channel) === targetExt) {
                        ami.action({ action: 'Hangup', channel: channel });
                    }
                });
            }
        });

        emitPeerUpdate(targetExt);
        refreshTrunksForExtension(targetExt);
        io.emit('call_summary', getCallSummary());
    });

    socket.on('action_hangup', ({ exten }) => {
        const targetExt = String(exten || '').trim();
        if (!targetExt) return;

        ami.action({ action: 'Command', command: 'core show channels concise' }, (err, res) => {
            if (err || !res) return;
            const output = normalizeAmiOut(res);
            output.split('\n').forEach(line => {
                if (!line.includes('!')) return;
                const channel = line.split('!')[0];
                if (extractPeer(channel) === targetExt) {
                    ami.action({ action: 'Hangup', channel: channel });
                }
            });
        });
    });
});

app.use(express.static(path.join(__dirname, 'public')));

const PORT = 3050;
server.listen(PORT, '127.0.0.1', () => {
    console.log(`Live Operator Panel listening on port ${PORT}`);
});
