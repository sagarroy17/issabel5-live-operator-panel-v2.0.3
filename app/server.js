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

const dbConfig = {
    host: 'localhost',
    user: 'root',
    password: "MYSQL_PASSWORD_PLACEHOLDER",
    database: 'asterisk'
};

const cdrDbConfig = {
    host: 'localhost',
    user: 'root',
    password: "MYSQL_PASSWORD_PLACEHOLDER",
    database: 'asteriskcdrdb'
};

const ami = new AsteriskManager(5038, "127.0.0.1", "admin", "AMI_PASSWORD_PLACEHOLDER", true);
ami.keepConnected();

// In-memory state stores
const peerStatuses = {};      // { peer: 'IDLE' | 'RINGING' | 'INCALL' }
const callStartTimes = {};    // { peer: timestamp_ms }
const callDetails = {};       // { peer: { dir: 'IN' | 'OUT', number: '017...' } }
const regStatus = {};         // { peer: { registered: bool, ip: string, regSince: timestamp_ms } }
const channelMap = {};        // { uniqueid: { peer, channel, callerNum, exten, direction, linkedid, context, isTrunk } }
const knownTrunks = new Set(); // Track configured trunk keys (case-insensitive via lowercase map)
const trunkCanonicalMap = {};  // { lowercase_trunk: canonical_trunk_key }
const knownExtensions = new Set(); // Track configured extension numbers

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

// Refresh known extensions & trunks sets from MySQL for accurate peer/trunk classification
async function refreshPeerRegistry() {
    try {
        const conn = await mysql.createConnection(dbConfig);
        const [extRows] = await conn.execute('SELECT extension FROM users');
        const [trunkRows] = await conn.execute('SELECT name, channelid FROM trunks WHERE disabled = "off"');
        await conn.end();

        knownExtensions.clear();
        extRows.forEach(r => {
            if (r.extension) knownExtensions.add(String(r.extension).trim());
        });

        knownTrunks.clear();
        Object.keys(trunkCanonicalMap).forEach(k => delete trunkCanonicalMap[k]);
        trunkRows.forEach(r => {
            const key = (r.channelid || r.name || '').trim();
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
        console.error('Error refreshing peer registry:', err.message);
    }
}
refreshPeerRegistry();
setInterval(refreshPeerRegistry, 30000);

function isTrunkPeer(peer) {
    if (!peer) return false;
    return knownTrunks.has(peer) || Boolean(trunkCanonicalMap[peer.toLowerCase()]);
}

function canonicalPeerName(rawPeer) {
    if (!rawPeer) return null;
    const trimmed = rawPeer.trim();
    if (trunkCanonicalMap[trimmed.toLowerCase()]) {
        return trunkCanonicalMap[trimmed.toLowerCase()];
    }
    return trimmed;
}

// Fetch Today's Processed Calls, Total Processed Calls & Per-Extension Daily Calls from asteriskcdrdb.cdr
async function refreshCdrStats() {
    try {
        const conn = await mysql.createConnection(cdrDbConfig);
        const [rows] = await conn.execute(`
            SELECT 
                COUNT(DISTINCT uniqueid) AS total_calls,
                COUNT(DISTINCT CASE WHEN DATE(calldate) = CURDATE() THEN uniqueid END) AS today_calls
            FROM cdr
        `);

        const [todayRows] = await conn.execute(`
            SELECT uniqueid, src, dst, channel, dstchannel
            FROM cdr
            WHERE DATE(calldate) = CURDATE()
        `);
        await conn.end();

        // Count distinct uniqueid calls per extension for today (both incoming & outgoing)
        const extCallSets = {};
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
        console.error('Error fetching CDR call stats:', err.message);
    }
}
setTimeout(refreshCdrStats, 800);
setInterval(refreshCdrStats, 5000);

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

// Extract peer/extension name from channel string (e.g., "PJSIP/101-0000001a" -> "101", "SIP/BOL ITSP-00001" -> "BOL ITSP")
function extractPeer(channel) {
    if (!channel) return null;
    const match = String(channel).match(/^(?:PJSIP|SIP|IAX2)\/(.+?)(?:-[0-9a-fA-F]+|@.*)?$/i);
    if (!match) return null;
    return canonicalPeerName(match[1]);
}

// Find correlated channels sharing the same linkedid
function getLinkedChannels(linkedid) {
    if (!linkedid) return [];
    return Object.values(channelMap).filter(c => c.linkedid === linkedid);
}

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

// Reconcile active channels to prevent stale states & ensure accurate direction
function syncActiveChannels() {
    ami.action({ action: 'Command', command: 'core show channels concise' }, (err, res) => {
        if (err || !res) return;
        const output = normalizeAmiOut(res);
        const activePeers = new Set();
        const activeUniqueIds = new Set();

        output.split('\n').forEach(line => {
            if (!line.includes('!')) return;
            const parts = line.split('!');
            const channel = parts[0];
            const context = (parts[1] || '').toLowerCase();
            const exten = parts[2] || '';
            const state = parts[4];
            const callerNum = parts[7];
            const uniqueid = parts[11] || channel;
            const connectedNum = parts[12] || exten;
            const peer = extractPeer(channel);

            activeUniqueIds.add(uniqueid);

            if (peer) {
                activePeers.add(peer);
                const trunkFlag = isTrunkPeer(peer);

                if (state === 'Up') {
                    if (peerStatuses[peer] !== 'INCALL') {
                        peerStatuses[peer] = 'INCALL';
                        if (!callStartTimes[peer]) callStartTimes[peer] = Date.now();
                    }
                    if (!callDetails[peer]) {
                        const remote = (connectedNum && connectedNum !== peer && connectedNum !== 's') ? connectedNum : callerNum;
                        if (remote && remote !== peer && remote !== 's') {
                            // If trunk and context is dialout/internal -> OUT; if extension and context is dialout/from-internal -> OUT
                            const isOut = context.includes('dialout') || context.includes('outbound') ||
                                          (!trunkFlag && context.includes('from-internal') && exten !== peer && exten !== 's');
                            callDetails[peer] = { dir: isOut ? 'OUT' : 'IN', number: remote };
                        }
                    }
                    emitPeerUpdate(peer);
                } else if (state === 'Ring' || state === 'Ringing') {
                    if (peerStatuses[peer] !== 'INCALL') {
                        peerStatuses[peer] = 'RINGING';
                        emitPeerUpdate(peer);
                    }
                }
            }
        });

        Object.keys(channelMap).forEach(uid => {
            if (!activeUniqueIds.has(uid) && !activePeers.has(channelMap[uid].peer)) {
                delete channelMap[uid];
            }
        });

        Object.keys(peerStatuses).forEach(peer => {
            if (peerStatuses[peer] !== 'IDLE' && !activePeers.has(peer)) {
                peerStatuses[peer] = 'IDLE';
                delete callStartTimes[peer];
                delete callDetails[peer];
                emitPeerUpdate(peer);
            }
        });

        io.emit('call_summary', getCallSummary());
    });
}

setInterval(pollRegistrations, 5000);
setInterval(syncActiveChannels, 4000);
setTimeout(() => {
    pollRegistrations();
    syncActiveChannels();
    broadcastSystemStats();
    refreshCdrStats();
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
            channelMap[uid] = {
                uniqueid: uid,
                linkedid,
                peer,
                channel: evt.channel,
                callerNum,
                exten: dialedExten,
                context,
                isTrunk: trunkFlag,
                state: evt.channelstatedesc || 'Ring'
            };
        }

        if (peer) {
            const linked = getLinkedChannels(linkedid);
            const originatingExtLeg = linked.find(c => !c.isTrunk && c.peer && (c.context.includes('from-internal') || c.context.includes('dialout')));
            const incomingTrunkLeg = linked.find(c => c.isTrunk && (c.context.includes('from-trunk') || c.context.includes('from-pstn') || c.context.includes('from-did')));

            if (trunkFlag) {
                // Trunk channel created: if linked to an outbound internal extension leg, trunk is OUT; otherwise IN
                if (originatingExtLeg || context.includes('dialout') || context.includes('from-internal') || context.includes('outbound')) {
                    const outNum = (originatingExtLeg && originatingExtLeg.exten && originatingExtLeg.exten !== 's')
                        ? originatingExtLeg.exten
                        : ((dialedExten && dialedExten !== 's') ? dialedExten : callerNum);
                    if (outNum) callDetails[peer] = { dir: 'OUT', number: outNum };
                } else {
                    const inNum = (callerNum && callerNum !== peer && callerNum !== 's') ? callerNum : dialedExten;
                    if (inNum) callDetails[peer] = { dir: 'IN', number: inNum };
                }
            } else {
                // Extension channel created
                if (incomingTrunkLeg || context.includes('ext-queues') || context.includes('ext-group') || context.includes('from-trunk') || context.includes('from-pstn')) {
                    const externalCaller = (incomingTrunkLeg && incomingTrunkLeg.callerNum) ? incomingTrunkLeg.callerNum : callerNum;
                    if (externalCaller && externalCaller !== peer && externalCaller !== 's') {
                        callDetails[peer] = { dir: 'IN', number: externalCaller };
                    }
                } else if (callerNum === peer && dialedExten && dialedExten !== 's' && dialedExten !== peer) {
                    callDetails[peer] = { dir: 'OUT', number: dialedExten };
                } else if (callerNum && callerNum !== peer && callerNum !== 's') {
                    callDetails[peer] = { dir: 'IN', number: callerNum };
                }
            }

            if (peerStatuses[peer] !== 'INCALL') {
                peerStatuses[peer] = 'RINGING';
            }
            emitPeerUpdate(peer);
        }
    }

    if (eventName === 'DialBegin') {
        const srcPeer = extractPeer(evt.channel);
        const destPeer = extractPeer(evt.destchannel);
        const srcCaller = evt.calleridnum || '';
        const destExten = evt.destcalleridnum || evt.dialstring || destPeer || '';
        const linkedid = evt.linkedid || evt.uniqueid;
        const linked = getLinkedChannels(linkedid);
        const incomingTrunkLeg = linked.find(c => c.isTrunk && (c.context.includes('from-trunk') || c.context.includes('from-pstn') || c.context.includes('from-did')));

        // 1. Source leg (e.g., Extension dialing out, or Queue/RingGroup dialing an agent)
        if (srcPeer && !isTrunkPeer(srcPeer)) {
            if (incomingTrunkLeg) {
                // Call originated from an external trunk (via IVR / Queue / Ring Group) -> Extension is receiving IN call
                const extCaller = incomingTrunkLeg.callerNum || srcCaller;
                if (extCaller && extCaller !== srcPeer && extCaller !== 's') {
                    callDetails[srcPeer] = { dir: 'IN', number: extCaller };
                    emitPeerUpdate(srcPeer);
                }
            } else if (destExten && destExten !== 's') {
                const cleanDest = destExten.replace(/^.*\/([^@]+).*$/, '$1');
                if (cleanDest !== srcPeer) {
                    callDetails[srcPeer] = { dir: 'OUT', number: cleanDest };
                    emitPeerUpdate(srcPeer);
                }
            }
        }

        // 2. Destination leg (either an Outbound Trunk being dialed OR an Extension receiving a call)
        if (destPeer) {
            if (isTrunkPeer(destPeer)) {
                // Trunk is being dialed by an internal extension -> Trunk shows OUT with dialed number
                const dialedNumber = (callDetails[srcPeer] && callDetails[srcPeer].number)
                    ? callDetails[srcPeer].number
                    : destExten.replace(/^.*\/([^@]+).*$/, '$1');
                if (dialedNumber && dialedNumber !== 's') {
                    callDetails[destPeer] = { dir: 'OUT', number: dialedNumber };
                }
            } else {
                // Extension is being dialed (direct, from Queue, or from Ring Group) -> Extension shows IN with external/internal caller
                const incomingNum = (incomingTrunkLeg && incomingTrunkLeg.callerNum)
                    ? incomingTrunkLeg.callerNum
                    : ((srcCaller && srcCaller !== destPeer && srcCaller !== 's') ? srcCaller : srcPeer);
                if (incomingNum) {
                    callDetails[destPeer] = { dir: 'IN', number: incomingNum };
                }
            }

            if (peerStatuses[destPeer] !== 'INCALL') {
                peerStatuses[destPeer] = 'RINGING';
            }
            emitPeerUpdate(destPeer);
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
            if (!callDetails[peer]) {
                const linked = getLinkedChannels(linkedid);
                const incomingTrunkLeg = linked.find(c => c.isTrunk && (c.context.includes('from-trunk') || c.context.includes('from-pstn')));
                const remote = (incomingTrunkLeg && incomingTrunkLeg.callerNum)
                    ? incomingTrunkLeg.callerNum
                    : ((connectedNum && connectedNum !== peer && connectedNum !== 's') ? connectedNum : callerNum);
                if (remote && remote !== peer && remote !== 's') {
                    callDetails[peer] = { dir: 'IN', number: remote };
                }
            }

            if (stateDesc === 'Up') {
                peerStatuses[peer] = 'INCALL';
                if (!callStartTimes[peer]) callStartTimes[peer] = Date.now();
                emitPeerUpdate(peer);
            } else if (stateDesc === 'Ringing' || stateDesc === 'Ring') {
                if (peerStatuses[peer] !== 'INCALL') {
                    peerStatuses[peer] = 'RINGING';
                    emitPeerUpdate(peer);
                }
            }
        }
    }

    if (eventName === 'BridgeEnter') {
        const peer = extractPeer(evt.channel);
        const uid = evt.uniqueid;
        const linkedid = evt.linkedid || (channelMap[uid] && channelMap[uid].linkedid);
        const connectedNum = evt.connectedlinenum || '';
        const callerNum = evt.calleridnum || '';

        if (peer) {
            peerStatuses[peer] = 'INCALL';
            if (!callStartTimes[peer]) callStartTimes[peer] = Date.now();
            if (!callDetails[peer]) {
                const linked = getLinkedChannels(linkedid);
                const incomingTrunkLeg = linked.find(c => c.isTrunk && (c.context.includes('from-trunk') || c.context.includes('from-pstn')));
                const remote = (incomingTrunkLeg && incomingTrunkLeg.callerNum)
                    ? incomingTrunkLeg.callerNum
                    : ((connectedNum && connectedNum !== peer && connectedNum !== 's') ? connectedNum : callerNum);
                if (remote && remote !== peer && remote !== 's') {
                    callDetails[peer] = { dir: 'IN', number: remote };
                }
            }
            emitPeerUpdate(peer);
        }
    }

    if (eventName === 'Hangup') {
        const peer = extractPeer(evt.channel);
        const uid = evt.uniqueid;
        if (uid) delete channelMap[uid];

        if (peer) {
            peerStatuses[peer] = 'IDLE';
            delete callStartTimes[peer];
            delete callDetails[peer];
            emitPeerUpdate(peer);
        }
        setTimeout(refreshCdrStats, 1000);
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

// REST API: Fetch extensions from MySQL
app.get('/api/extensions', async (req, res) => {
    try {
        const conn = await mysql.createConnection(dbConfig);
        const [rows] = await conn.execute('SELECT extension, name FROM users ORDER BY CAST(extension AS UNSIGNED) ASC');
        await conn.end();
        res.json(rows);
    } catch (err) {
        console.error('Error fetching extensions:', err);
        res.status(500).json({ error: err.message });
    }
});

// REST API: Fetch trunks from MySQL
app.get('/api/trunks', async (req, res) => {
    try {
        const conn = await mysql.createConnection(dbConfig);
        const [rows] = await conn.execute('SELECT trunkid, name, tech, channelid FROM trunks WHERE disabled = "off" ORDER BY trunkid ASC');
        await conn.end();
        const formatted = rows.map(r => ({
            key: r.channelid || r.name,
            name: r.name,
            tech: r.tech
        }));
        res.json(formatted);
    } catch (err) {
        console.error('Error fetching trunks:', err);
        res.status(500).json({ error: err.message });
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

    // Reset hung extension state
    socket.on('action_reset', ({ exten }) => {
        if (!exten) return;
        peerStatuses[exten] = 'IDLE';
        delete callStartTimes[exten];
        delete callDetails[exten];
        Object.keys(channelMap).forEach(uid => {
            if (channelMap[uid].peer === exten) delete channelMap[uid];
        });
        emitPeerUpdate(exten);
    });

    // Hangup active call on target extension
    socket.on('action_hangup', ({ exten }) => {
        if (!exten) return;
        ami.action({ action: 'Command', command: 'core show channels concise' }, (err, res) => {
            if (err || !res) return;
            const output = normalizeAmiOut(res);
            output.split('\n').forEach(line => {
                if (!line.includes('!')) return;
                const channel = line.split('!')[0];
                if (extractPeer(channel) === exten) {
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
