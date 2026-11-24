# Issabel 5 Live Operator Panel (FOP2 Style) - v2.0.3

A lightweight, real-time web-based Live Operator Panel engineered specifically for **Issabel 5** (Asterisk 18 / PJSIP & SIP). Monitor extensions and trunks in an Elastix 4-inspired compact card grid with live call direction & remote party badges, CDR call counters, real-time server telemetry, Light/Dark theme switching, and one-click call controls.

---

## Direct Download Links

- **Repository URL:** https://github.com/sagarroy17/issabel5-live-operator-panel-v2.0.3
- **Download ZIP:** https://github.com/sagarroy17/issabel5-live-operator-panel-v2.0.3/archive/refs/heads/main.zip
- **Download TAR.GZ:** https://github.com/sagarroy17/issabel5-live-operator-panel-v2.0.3/archive/refs/heads/main.tar.gz

---

## One-Line Installation on Any Issabel 5 Server

Log in to your Issabel 5 server via SSH as `root` and run the following single command:

```bash
rm -rf /tmp/op-panel && mkdir -p /tmp/op-panel && curl -sL [https://github.com/sagarroy17/issabel5-live-operator-panel-v2.0.3/archive/refs/heads/main.tar.gz](https://github.com/sagarroy17/issabel5-live-operator-panel-v2.0.3/archive/refs/heads/main.tar.gz) | tar -xz -C /tmp/op-panel --strip-components=1 && bash /tmp/op-panel/install.sh

Single link-
```bash
rm -rf /tmp/op-panel && mkdir -p /tmp/op-panel && curl -sL https://github.com/sagarroy17/issabel5-live-operator-panel-v2.0.3/archive/refs/heads/main.tar.gz | tar -xz -C /tmp/op-panel --strip-components=1 && bash /tmp/op-panel/install.sh


After installation completes (Installation Complete!), log in to your Issabel Web GUI and navigate to PBX -> Live Operator Panel.

Key Features
Elastix 4 Compact Card Grid: High-density responsive layout for extensions and SIP/IAX/PJSIP trunks (supports trunk names containing spaces and PJSIP Avail states).

Live Call Direction & Remote Party Number: Displays real-time call direction badges (IN: <number> / OUT: <number>) directly on active extension and trunk cards.

Live & CDR Call Counters: Real-time Active Calls, Today's Processed Calls, and Total Processed Calls queried directly from asteriskcdrdb.cdr.

Dual Real-Time Timers: Registration Uptime (Up HH:MM:SS) with endpoint IP address and Active Call Duration (MM:SS).

One-Click Operator Controls: Instant Hangup and Reset controls on every extension card.

Transparent Server Monitor & Clock Widget: Fixed bottom-right HUD showing live CPU, RAM, Storage, Local IP, Public IP, Location, and Clock.

Light / Dark Theme Switcher: One-click toggle between Light Mode and Dark Mode (persisted in localStorage).

Zero-Configuration Auto Setup: Automatically detects MySQL root credentials from /etc/issabel.conf, Asterisk AMI credentials from /etc/asterisk/manager.conf, and exact PBX menu parent IDs (pbxconfig / pbx).

Service Management & Troubleshooting

Check service status:

Bash
systemctl status operator-panel
Restart service:

Bash
systemctl restart operator-panel
View live logs:

Bash
journalctl -u operator-panel -f

#Uninstall (if ever needed):

Bash
systemctl stop operator-panel && systemctl disable operator-panel
rm -rf /opt/custom-operator-panel /var/www/html/modules/live_operator_panel /etc/systemd/system/operator-panel.service
sqlite3 /var/www/db/menu.db "DELETE FROM menu WHERE id='live_operator_panel';"
sqlite3 /var/www/db/acl.db "DELETE FROM acl_group_permission WHERE id_resource IN (SELECT id FROM acl_resource WHERE name='live_operator_panel');"
sqlite3 /var/www/db/acl.db "DELETE FROM acl_resource WHERE name='live_operator_panel';"
sed -i '/op-panel/d;/HTTP:Upgrade/d' /etc/httpd/conf.d/ssl.conf
systemctl daemon-reload && systemctl restart httpd

Done!
