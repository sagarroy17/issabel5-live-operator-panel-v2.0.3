const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const AsteriskManager = require('asterisk-manager');
const { exec } = require('child_process');
const os = require('os');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

const dbConfig = {
    host: '127.0.0.1',
    user: 'root',
    password: "MYSQL_PASSWORD_PLACEHOLDER",
    database: 'asterisk',
    waitForConnections: true,
    connectionLimit: 5
};

const cdrDbConfig = {
    ...dbConfig,
    database: 'asteriskcdrdb'
};

let pool;
let cdrPool;
let ami;
let amiConnected = false;

const state = {
    peers: {},
    activeChannels: {},
    cdrStats: { today: 0, total: 0, perExtToday: {} },
    systemStats: {
        cpu: 0,
        cores: os.cpus().length,
        ramUsed: '0',
        ramTotal: (os.totalmem() / 1073741824).toFixed(2),
        ramPercent: 0,
        diskUsed: '0G',
        diskFree: '0G',
        diskPercent: 0,
        localIp: '127.0.0.1',
        publicIp: 'Detecting...',
        location: 'Dhaka, BD'
    }
};

function runCmd(cmd) {
    return new Promise((resolve) => {
        exec(cmd, { timeout: 4000 }, (err, stdout) => {
            resolve(err ? '' : stdout.trim());
        });
    });
}

async function initDatabases() {
    pool = mysql.createPool(dbConfig);
    cdrPool = mysql.createPool(cdrDbConfig);
}

async function loadPeersFromDB() {
    try {
        const [users] = await pool.query('SELECT extension, name FROM users ORDER BY CAST(extension AS UNSIGNED), extension');
        const [trunks] = await pool.query('SELECT trunkid, name, tech, channelid, disabled FROM trunks WHERE disabled = "off" ORDER BY trunkid');

        const freshPeers = {};

        for (const u of users) {
            const ext = String(u.extension).trim();
            freshPeers[ext] = {
                id: ext,
                name: u.name || ext,
                type: 'extension',
                tech: 'PJSIP/SIP',
                status: 'Unregistered',
                ip: 'No IP',
                regTime: null,
                callState: 'IDLE',
                callDirection: null,
                remoteParty: '',
                callStartTime: null,
                dailyCalls: state.cdrStats.perExtToday[ext] || 0
            };
        }

        for (const t of trunks) {
            const key = `trunk_${t.trunkid}`;
            const chanId = (t.channelid || t.name || '').trim();
            freshPeers[key] = {
                id: key,
                trunkName: t.name || chanId,
                channelId: chanId,
                type: 'trunk',
                tech: (t.tech || 'SIP').toUpperCase(),
                status: 'Offline',
                ip: 'Trunk',
                regTime: null,
                callState: 'IDLE',
                callDirection: null,
                remoteParty: '',
                callStartTime: null
            };
        }

        // Preserve existing runtime registration & call states
        for (const [k, v] of Object.entries(freshPeers)) {
            if (state.peers[k]) {
                v.status = state.peers[k].status;
                v.ip = state.peers[k].ip;
                v.regTime = state.peers[k].regTime;
                v.tech = state.peers[k].tech || v.tech;
            }
        }
        state.peers = freshPeers;
    } catch (e) {
        console.error('DB Peer Load Error:', e.message);
    }
}

async function pollRegistrationStatus() {
    try {
        const [pjsipOut, sipOut] = await Promise.all([
            runCmd('asterisk -rx "pjsip show endpoints"'),
            runCmd('asterisk -rx "sip show peers"')
        ]);

        const now = Date.now();
        const registeredMap = {};

        // Parse PJSIP endpoints (handles Avail, Not in use, In use, Ringing, Busy, etc.)
        if (pjsipOut) {
            const lines = pjsipOut.split('\n');
            let currentEndpoint = null;
            for (const line of lines) {
                const epMatch = line.match(/^\s*Endpoint:\s+([^\s\/]+)(?:\/\S+)?\s+(.+)/);
                if (epMatch) {
                    currentEndpoint = epMatch[1].trim();
                    const rest = epMatch[2];
                    if (/Avail|Not in use|In use|Ringing|Busy/i.test(rest) && !/Unavailable/i.test(rest)) {
                        registeredMap[currentEndpoint] = { status: 'Registered', ip: 'PJSIP', tech: 'PJSIP' };
                    }
                }
                const contactMatch = line.match(/^\s*Contact:\s+([^\s\/]+)\/sip:(?:[^@]+@)?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)(?::(\d+))?.*?(Avail|NonQual)/i);
                if (contactMatch) {
                    const ep = contactMatch[1].trim();
                    const ip = contactMatch[2];
                    registeredMap[ep] = { status: 'Registered', ip, tech: 'PJSIP' };
                }
            }
        }

        // Parse Legacy Chan_SIP peers (handles names with spaces like "BOL ITSP")
        if (sipOut) {
            const lines = sipOut.split('\n');
            for (const line of lines) {
                const m = line.match(/^(.+?)\s+(\d+\.\d+\.\d+\.\d+|\(Unspecified\))\s+.*?\b(OK|Unmonitored)\b/i);
                if (m) {
                    let namePart = m[1].trim();
                    if (namePart.includes('/')) {
                        namePart = namePart.split('/')[0].trim();
                    }
                    const ip = m[2] === '(Unspecified)' ? 'Unmonitored' : m[2];
                    if (ip !== 'Unmonitored' || m[3].toUpperCase() === 'OK') {
                        registeredMap[namePart] = { status: 'Registered', ip, tech: 'SIP' };
                    }
                }
            }
        }

        for (const peer of Object.values(state.peers)) {
            const matchKeys = peer.type === 'extension'
                ? [peer.id]
                : [peer.channelId, peer.trunkName].filter(Boolean);

            let found = null;
            for (const k of matchKeys) {
                if (registeredMap[k]) {
                    found = registeredMap[k];
                    break;
                }
                const lowerK = k.toLowerCase();
                for (const [rk, rv] of Object.entries(registeredMap)) {
                    if (rk.toLowerCase() === lowerK) {
                        found = rv;
                        break;
                    }
                }
                if (found) break;
            }

            if (found) {
                if (peer.status !== 'Registered') {
                    peer.regTime = now;
                }
                peer.status = 'Registered';
                peer.ip = found.ip;
                peer.tech = found.tech;
            } else {
                peer.status = peer.type === 'trunk' ? 'Offline' : 'Unregistered';
                peer.ip = peer.type === 'trunk' ? 'Trunk' : 'No IP';
                peer.regTime = null;
            }
        }
    } catch (e) {
        console.error('Poll Registration Error:', e.message);
    }
}

function findPeerByChannel(channelName) {
    if (!channelName) return null;
    // Channel format: PJSIP/101-00000001 or SIP/BOL ITSP-00000002
    const techSplit = channelName.split('/');
    if (techSplit.length < 2) return null;
    const remainder = techSplit.slice(1).join('/');
    const lastDash = remainder.lastIndexOf('-');
    const endpointName = (lastDash > 0 ? remainder.substring(0, lastDash) : remainder).trim();

    if (state.peers[endpointName]) return state.peers[endpointName];

    const lowerEp = endpointName.toLowerCase();
    for (const peer of Object.values(state.peers)) {
        if (peer.type === 'extension' && peer.id.toLowerCase() === lowerEp) return peer;
        if (peer.type === 'trunk') {
            if ((peer.channelId && peer.channelId.toLowerCase() === lowerEp) ||
                (peer.trunkName && peer.trunkName.toLowerCase() === lowerEp)) {
                return peer;
            }
        }
    }
    return null;
}

async function pollActiveChannels() {
    try {
        const out = await runCmd('asterisk -rx "core show channels concise"');
        const now = Date.now();

        // Reset call states before recomputing from live channels
        for (const peer of Object.values(state.peers)) {
            peer.callState = 'IDLE';
            peer.callDirection = null;
            peer.remoteParty = '';
            peer.callStartTime = null;
            if (peer.type === 'extension') {
                peer.dailyCalls = state.cdrStats.perExtToday[peer.id] || 0;
            }
        }

        if (!out) {
            state.activeChannels = {};
            return;
        }

        const lines = out.split('\n').filter(Boolean);
        const channels = [];

        for (const line of lines) {
            // Concise format: Channel!Context!Extension!Priority!State!Application!Data!CallerID!AccountCode!PeerAccount!AmaFlags!Duration!BridgeId!Uniqueid
            const parts = line.split('!');
            if (parts.length < 13) continue;
            channels.push({
                channel: parts[0],
                context: (parts[1] || '').toLowerCase(),
                exten: parts[2] || '',
                state: parts[4] || '',
                app: parts[5] || '',
                appData: parts[6] || '',
                callerId: parts[7] || '',
                durationSec: parseInt(parts[11], 10) || 0,
                bridgeId: parts[12] || ''
            });
        }

        // Group channels by BridgeId or Linked channels to correlate Extension <-> Trunk legs accurately
        const bridgeMap = {};
        for (const ch of channels) {
            if (ch.bridgeId) {
                if (!bridgeMap[ch.bridgeId]) bridgeMap[ch.bridgeId] = [];
                bridgeMap[ch.bridgeId].push(ch);
            }
        }

        for (const ch of channels) {
            const peer = findPeerByChannel(ch.channel);
            if (!peer) continue;

            const st = ch.state.toLowerCase();
            let mappedState = 'IN CALL';
            if (st === 'ringing' || st === 'ring') mappedState = 'RINGING';
            else if (st === 'up') mappedState = 'IN CALL';
            else if (st === 'dialing') mappedState = 'DIALING';

            // Determine accurate call direction & remote party
            let direction = 'IN';
            let remote = '';

            // Check paired channel in the same bridge first (most accurate for Extension <-> Trunk correlation)
            const bridgedPeers = (ch.bridgeId && bridgeMap[ch.bridgeId]) ? bridgeMap[ch.bridgeId] : [];
            const otherLeg = bridgedPeers.find(b => b.channel !== ch.channel);

            if (peer.type === 'trunk') {
                // Outbound if context indicates dialout/internal OR other leg is an internal extension originating the call
                const isOutbound = ch.context.includes('dialout') ||
                    ch.context.includes('from-internal') ||
                    ch.context.includes('outbound') ||
                    st === 'dialing' ||
                    (otherLeg && (otherLeg.context.includes('from-internal') || otherLeg.context.includes('dialout')));

                if (isOutbound) {
                    direction = 'OUT';
                    remote = ch.exten && ch.exten !== 's' ? ch.exten : (ch.callerId || '');
                } else {
                    direction = 'IN';
                    remote = ch.callerId || ch.exten || '';
                }
            } else {
                // Extension leg
                const isInboundQueueOrRingGroup = ch.context.includes('ext-queues') ||
                    ch.context.includes('ext-group') ||
                    ch.context.includes('from-trunk') ||
                    ch.context.includes('from-pstn') ||
                    ch.context.includes('from-did') ||
                    st === 'ringing';

                if (isInboundQueueOrRingGroup) {
                    direction = 'IN';
                    remote = (otherLeg && otherLeg.callerId) ? otherLeg.callerId : (ch.callerId || '');
                } else if (ch.context.includes('from-internal') || ch.context.includes('dialout') || st === 'ring' || st === 'dialing') {
                    // Check if another trunk leg is bridged: if trunk entered from-trunk, then extension is receiving an IN call
                    const trunkLeg = bridgedPeers.map(b => ({ ch: b, p: findPeerByChannel(b.channel) })).find(x => x.p && x.p.type === 'trunk');
                    if (trunkLeg && (trunkLeg.ch.context.includes('from-trunk') || trunkLeg.ch.context.includes('from-pstn'))) {
                        direction = 'IN';
                        remote = trunkLeg.ch.callerId || ch.callerId || '';
                    } else {
                        direction = 'OUT';
                        remote = (ch.exten && ch.exten !== 's' && ch.exten !== peer.id) ? ch.exten : (otherLeg ? otherLeg.exten : ch.callerId);
                    }
                } else {
                    direction = 'IN';
                    remote = ch.callerId || '';
                }
            }

            // Clean remote string (strip quotes/brackets if any)
            remote = String(remote || '').replace(/["<>]/g, '').trim();

            peer.callState = mappedState;
            peer.callDirection = direction;
            peer.remoteParty = remote;
            peer.callStartTime = now - (ch.durationSec * 1000);
        }

        state.activeChannels = channels;
    } catch (e) {
        console.error('Poll Channels Error:', e.message);
    }
}

async function pollCdrStats() {
    try {
        const [[todayRow]] = await cdrPool.query('SELECT COUNT(*) AS cnt FROM cdr WHERE DATE(calldate) = CURDATE()');
        const [[totalRow]] = await cdrPool.query('SELECT COUNT(*) AS cnt FROM cdr');

        // Per-extension daily call count (matches src or dst or channel/dstchannel for today)
        const [rows] = await cdrPool.query(`
            SELECT src, dst, channel, dstchannel
            FROM cdr
            WHERE DATE(calldate) = CURDATE()
        `);

        const extCounts = {};
        const extSet = new Set(
            Object.values(state.peers)
                .filter(p => p.type === 'extension')
                .map(p => p.id)
        );

        for (const r of rows) {
            const matchedExts = new Set();
            const src = String(r.src || '').trim();
            const dst = String(r.dst || '').trim();

            if (extSet.has(src)) matchedExts.add(src);
            if (extSet.has(dst)) matchedExts.add(dst);

            // Also extract extension from channel/dstchannel (e.g., PJSIP/101-000001)
            for (const chStr of [r.channel, r.dstchannel]) {
                if (!chStr) continue;
                const m = String(chStr).match(/^(?:PJSIP|SIP|IAX2)\/([^-\s@]+)-/i);
                if (m && extSet.has(m[1])) {
                    matchedExts.add(m[1]);
                }
            }

            for (const ext of matchedExts) {
                extCounts[ext] = (extCounts[ext] || 0) + 1;
            }
        }

        state.cdrStats = {
            today: todayRow ? todayRow.cnt : 0,
            total: totalRow ? totalRow.cnt : 0,
            perExtToday: extCounts
        };

        for (const peer of Object.values(state.peers)) {
            if (peer.type === 'extension') {
                peer.dailyCalls = extCounts[peer.id] || 0;
            }
        }
    } catch (e) {
        console.error('CDR Poll Error:', e.message);
    }
}

async function pollSystemStats() {
    try {
        const totalMem = os.totalmem();
        const freeMem = os.freemem();
        const usedMem = totalMem - freeMem;

        const [cpuOut, dfOut, ipOut] = await Promise.all([
            runCmd("top -bn1 | grep 'Cpu(s)' | awk '{print $2 + $4}'"),
            runCmd("df -h / | awk 'NR==2 {print $3\"|\"$4\"|\"$5}'"),
            runCmd("hostname -I | awk '{print $1}'")
        ]);

        const [dUsed, dFree, dPct] = (dfOut || '0G|0G|0%').split('|');

        state.systemStats.cpu = Math.min(100, Math.round(parseFloat(cpuOut) || 0));
        state.systemStats.ramUsed = (usedMem / 1073741824).toFixed(2);
        state.systemStats.ramTotal = (totalMem / 1073741824).toFixed(2);
        state.systemStats.ramPercent = Math.round((usedMem / totalMem) * 100);
        state.systemStats.diskUsed = dUsed || '0G';
        state.systemStats.diskFree = dFree || '0G';
        state.systemStats.diskPercent = parseInt((dPct || '0').replace('%', ''), 10) || 0;
        if (ipOut) state.systemStats.localIp = ipOut;
    } catch (e) {
        // ignore transient errors
    }
}

async function detectPublicIp() {
    const pub = await runCmd('curl -s --max-time 4 https://api.ipify.org');
    if (pub && /^\d+\.\d+\.\d+\.\d+$/.test(pub)) {
        state.systemStats.publicIp = pub;
    }
}

function connectAMI() {
    ami = new AsteriskManager(5038, '127.0.0.1', 'admin', "AMI_PASSWORD_PLACEHOLDER", true);
    ami.keepConnected();

    ami.on('connect', () => {
        amiConnected = true;
    });

    ami.on('close', () => {
        amiConnected = false;
    });

    ami.on('error', () => {
        amiConnected = false;
    });

    ami.on('managerevent', (evt) => {
        const ev = (evt.event || '').toLowerCase();
        if (['newchannel', 'newstate', 'hangup', 'bridgeenter', 'bridgeleave', 'dialbegin', 'dialend'].includes(ev)) {
            pollActiveChannels();
            if (ev === 'hangup') {
                setTimeout(pollCdrStats, 1200);
            }
        }
        if (['peerstatus', 'contactstatus'].includes(ev)) {
            pollRegistrationStatus();
        }
    });
}

io.on('connection', (socket) => {
    socket.emit('state_update', getPayload());

    socket.on('hangup_ext', async (extId) => {
        const out = await runCmd('asterisk -rx "core show channels concise"');
        for (const line of out.split('\n')) {
            const ch = line.split('!')[0];
            if (ch && (ch.includes(`/${extId}-`) || ch.toLowerCase().includes(`/${String(extId).toLowerCase()}-`))) {
                await runCmd(`asterisk -rx "channel request hangup ${ch}"`);
            }
        }
        setTimeout(pollActiveChannels, 500);
    });

    socket.on('reset_ext', async (extId) => {
        await runCmd(`asterisk -rx "pjsip send notify reboot-yealink endpoint ${extId}"`);
        await runCmd(`asterisk -rx "sip notify reboot-yealink ${extId}"`);
        await pollRegistrationStatus();
        io.emit('state_update', getPayload());
    });
});

function getPayload() {
    const peersList = Object.values(state.peers);
    const activeCalls = Math.floor(
        peersList.filter(p => p.type === 'extension' && p.callState !== 'IDLE').length ||
        peersList.filter(p => p.type === 'trunk' && p.callState !== 'IDLE').length
    );

    return {
        amiConnected,
        activeCalls,
        todayCalls: state.cdrStats.today,
        totalCalls: state.cdrStats.total,
        peers: peersList,
        systemStats: state.systemStats,
        serverTime: Date.now()
    };
}

(async () => {
    await initDatabases();
    await loadPeersFromDB();
    await pollCdrStats();
    await pollRegistrationStatus();
    await pollActiveChannels();
    await pollSystemStats();
    detectPublicIp();
    connectAMI();

    setInterval(async () => {
        await pollActiveChannels();
        io.emit('state_update', getPayload());
    }, 1000);

    setInterval(async () => {
        await pollRegistrationStatus();
        await pollSystemStats();
    }, 5000);

    setInterval(async () => {
        await loadPeersFromDB();
        await pollCdrStats();
    }, 15000);

    server.listen(3050, '127.0.0.1', () => {
        console.log('Live Operator Panel running on http://127.0.0.1:3050');
    });
})();
