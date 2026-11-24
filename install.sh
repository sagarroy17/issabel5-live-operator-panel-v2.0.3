#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="/opt/custom-operator-panel"
ISSABEL_MOD_DIR="/var/www/html/modules/live_operator_panel"

echo "[1/6] Checking Node.js..."
if ! command -v node &> /dev/null || [[ "$(node -v | cut -d'.' -f1 | tr -d 'v')" -lt 18 ]]; then
    curl -fsSL https://rpm.nodesource.com/setup_20.x | bash -
    yum install -y nodejs
fi

echo "[2/6] Copying Application Files..."
mkdir -p "$APP_DIR/public"
cp -f "$SCRIPT_DIR/app/package.json" "$APP_DIR/package.json"
cp -f "$SCRIPT_DIR/app/server.js" "$APP_DIR/server.js"
cp -f "$SCRIPT_DIR/app/public/index.html" "$APP_DIR/public/index.html"

echo "[3/6] Auto-configuring MySQL & Asterisk AMI Credentials..."
node -e '
const fs = require("fs");
let mysqlPass = "admin";
if (fs.existsSync("/etc/issabel.conf")) {
    const m = fs.readFileSync("/etc/issabel.conf", "utf8").match(/^mysqlrootpwd\s*=\s*["'\'']?([^"'\''\r\n]+)["'\'']?/m);
    if (m) mysqlPass = m[1].trim();
}
let amiPass = "admin";
if (fs.existsSync("/etc/asterisk/manager.conf")) {
    const blk = fs.readFileSync("/etc/asterisk/manager.conf", "utf8").split("[admin]")[1] || "";
    const a = blk.match(/^secret\s*=\s*["'\'']?([^"'\''\r\n;]+)["'\'']?/m);
    if (a) amiPass = a[1].trim();
}
let code = fs.readFileSync("/opt/custom-operator-panel/server.js", "utf8");
code = code.replace(/password:\s*["'\'']MYSQL_PASSWORD_PLACEHOLDER["'\'']/g, "password: " + JSON.stringify(mysqlPass));
code = code.replace(/new AsteriskManager\(5038,\s*["'\'']127\.0\.0\.1["'\''],\s*["'\'']admin["'\''],\s*["'\'']AMI_PASSWORD_PLACEHOLDER["'\''],\s*true\)/g, "new AsteriskManager(5038, \"127.0.0.1\", \"admin\", " + JSON.stringify(amiPass) + ", true)");
fs.writeFileSync("/opt/custom-operator-panel/server.js", code);
'

echo "[4/6] Installing Node Dependencies & Starting Service..."
cd "$APP_DIR" && npm install --production --silent

cat << 'SVCEOF' > /etc/systemd/system/operator-panel.service
[Unit]
Description=Custom Live Operator Panel
After=network.target asterisk.service mariadb.service
[Service]
Type=simple
User=root
WorkingDirectory=/opt/custom-operator-panel
ExecStart=/usr/bin/node /opt/custom-operator-panel/server.js
Restart=always
RestartSec=3
[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable operator-panel
systemctl restart operator-panel

echo "[5/6] Registering Issabel Web GUI Module..."
mkdir -p "$ISSABEL_MOD_DIR"
cat << 'PHPEOF' > "$ISSABEL_MOD_DIR/index.php"
<?php
function _moduleContent(&$smarty, $module_name)
{
    return '<div style="margin:0; padding:0; width:100%; height:calc(100vh - 70px); overflow:hidden;">
                <iframe src="/op-panel/" style="width:100%; height:100%; border:none; display:block;" allowfullscreen></iframe>
            </div>';
}
?>
PHPEOF
chown -R asterisk:asterisk "$ISSABEL_MOD_DIR"

# Auto-detect exact PBX parent ID (pbxconfig or pbx)
PBX_PARENT=$(sqlite3 /var/www/db/menu.db "SELECT IdParent FROM menu WHERE id IN ('pbxadmin','voicemail','conference','fop2') AND IdParent != '' LIMIT 1;")
[ -z "$PBX_PARENT" ] && PBX_PARENT="pbxconfig"

# Remove any duplicate or old entries before inserting cleanly
sqlite3 /var/www/db/menu.db "DELETE FROM menu WHERE Name='Live Operator Panel' OR id='live_operator_panel';"
sqlite3 /var/www/db/menu.db "INSERT INTO menu (id, IdParent, Link, Name, Type, order_no) VALUES ('live_operator_panel', '$PBX_PARENT', '', 'Live Operator Panel', 'module', 11);"

ID_RES=$(sqlite3 /var/www/db/acl.db "SELECT id FROM acl_resource WHERE name='live_operator_panel';")
if [ -z "$ID_RES" ]; then
    sqlite3 /var/www/db/acl.db "INSERT INTO acl_resource (name, description) VALUES ('live_operator_panel', 'Live Operator Panel');"
    ID_RES=$(sqlite3 /var/www/db/acl.db "SELECT id FROM acl_resource WHERE name='live_operator_panel';")
fi
sqlite3 /var/www/db/acl.db "INSERT OR IGNORE INTO acl_group_permission (id_action, id_group, id_resource) VALUES (1, 1, $ID_RES);"

chown -R asterisk:asterisk /var/www/db /var/www/html/var
chmod 775 /var/www/db
chmod 664 /var/www/db/*.db
rm -rf /var/www/html/var/templates_c/* /var/lib/php/session/*

echo "[6/6] Configuring Apache Reverse Proxy..."
SSL_CONF="/etc/httpd/conf.d/ssl.conf"
if [ -f "$SSL_CONF" ]; then
    sed -i '/op-panel/d' "$SSL_CONF"
    sed -i '/HTTP:Upgrade/d' "$SSL_CONF"
    sed -i '/<\/VirtualHost>/i \
    RewriteEngine On\
    RewriteCond %{HTTP:Upgrade} =websocket [NC]\
    RewriteRule ^/op-panel/socket.io/(.*) ws://127.0.0.1:3050/socket.io/$1 [P,L]\
    ProxyPass /op-panel/socket.io/ http://127.0.0.1:3050/socket.io/\
    ProxyPassReverse /op-panel/socket.io/ http://127.0.0.1:3050/socket.io/\
    ProxyPass /op-panel/ http://127.0.0.1:3050/\
    ProxyPassReverse /op-panel/ http://127.0.0.1:3050/' "$SSL_CONF"
fi

httpd -t && systemctl restart httpd
echo "Installation Complete! Please log in to Issabel Web GUI and go to PBX -> Live Operator Panel."
