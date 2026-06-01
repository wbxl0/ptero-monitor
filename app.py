#!/usr/bin/env python3
"""
翼龙面板服务器监控保活工具
Pterodactyl Panel Server Monitor & Auto-Restart
"""

import os
import json
import asyncio
import sqlite3
import time
import logging
import base64
import hashlib
import html
import secrets
from datetime import datetime, timezone, timedelta
from aiohttp import web
import aiohttp
from typing import Dict, Optional
from curl_cffi.requests import AsyncSession
import subprocess
import shutil
import tempfile
from urllib.parse import parse_qs, unquote

# 配置
PORT = int(os.environ.get('PORT', 8000))
DB_PATH = os.environ.get('DB_PATH', 'monitor.db')
BEIJING_TZ = timezone(timedelta(hours=8))
DB_TIMEOUT = 30
LOG_RETENTION_DAYS = 2

def beijing_time_converter(timestamp):
    return datetime.fromtimestamp(timestamp, BEIJING_TZ).timetuple()

# 日志配置
logging.Formatter.converter = beijing_time_converter
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 全局监控任务存储
monitor_tasks: Dict[int, asyncio.Task] = {}

def init_db():
    """初始化数据库"""
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT)
    configure_db(conn)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS servers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            api_url TEXT NOT NULL,
            api_key TEXT NOT NULL,
            server_id TEXT NOT NULL,
            interval INTEGER DEFAULT 10,
            enabled INTEGER DEFAULT 1,
            last_status TEXT DEFAULT 'unknown',
            last_check TEXT,
            restart_count INTEGER DEFAULT 0,
            proxy_url TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # 尝试添加 proxy_url 字段（如果表已存在）
    try:
        c.execute('ALTER TABLE servers ADD COLUMN proxy_url TEXT DEFAULT ""')
    except:
        pass
    c.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_id INTEGER,
            action TEXT,
            status TEXT,
            message TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (server_id) REFERENCES servers(id)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at)')
    c.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()
    conn.close()

def get_setting(key: str, default: str = None) -> str:
    """获取设置"""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key = ?', (key,))
    row = c.fetchone()
    conn.close()
    return row['value'] if row else default

def set_setting(key: str, value: str):
    """保存设置"""
    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
    conn.commit()
    conn.close()

def get_db():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT)
    conn.row_factory = sqlite3.Row
    configure_db(conn)
    return conn

def configure_db(conn):
    """配置 SQLite 连接，降低并发读写时锁冲突概率。"""
    conn.execute('PRAGMA busy_timeout = 30000')
    conn.execute('PRAGMA journal_mode = WAL')
    conn.execute('PRAGMA foreign_keys = ON')

def now_beijing() -> str:
    """返回北京时间 ISO 字符串。"""
    return datetime.now(BEIJING_TZ).isoformat()

def parse_server_url(full_url: str) -> tuple:
    """从完整URL解析出base_url和server_id
    例如: https://cp.host2play.gratis/api/client/servers/c271ea0c-85dd-49c2-b5c7-154c608f6f49
    返回: (base_url, server_id)
    """
    full_url = full_url.rstrip('/')
    # 找到 /api/client/servers/ 后面的部分作为 server_id
    if '/api/client/servers/' in full_url:
        parts = full_url.split('/api/client/servers/')
        base_url = parts[0] + '/api/client/servers'
        server_id = parts[1].split('/')[0]  # 取第一段作为server_id
        return base_url, server_id
    else:
        # 尝试取最后一段作为 server_id
        parts = full_url.rsplit('/', 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return full_url, ''

def build_server_endpoint(api_url: str, server_id: str = None) -> tuple:
    """返回服务器 API 端点和 server_id，避免重复拼接 server_id。"""
    api_url = api_url.rstrip('/')
    if not server_id or server_id == '-':
        base_url, parsed_server_id = parse_server_url(api_url)
        return f"{base_url.rstrip('/')}/{parsed_server_id}", parsed_server_id

    if '/api/client/servers/' in api_url:
        base_url, parsed_server_id = parse_server_url(api_url)
        if parsed_server_id == server_id:
            return f"{base_url.rstrip('/')}/{server_id}", server_id

    if '{SERVER_ID}' in api_url:
        return api_url.replace('{SERVER_ID}', server_id).rstrip('/'), server_id

    if api_url.endswith('/api/client/servers'):
        return f"{api_url}/{server_id}", server_id

    if api_url.endswith(f'/{server_id}'):
        return api_url, server_id

    return f"{api_url}/{server_id}", server_id

def build_api_headers(api_key: str, api_url: str) -> dict:
    """构造翼龙 Client API 请求头。"""
    return {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

def format_http_error(status: int, text: str, headers: dict = None) -> str:
    """返回包含关键信息的 HTTP 错误。"""
    extra = ''
    if headers:
        server = headers.get('server') or headers.get('Server')
        cf_ray = headers.get('cf-ray') or headers.get('CF-Ray')
        parts = []
        if server:
            parts.append(f'server={server}')
        if cf_ray:
            parts.append(f'cf-ray={cf_ray}')
        if parts:
            extra = ' (' + ', '.join(parts) + ')'
    return f'HTTP {status}{extra}: {text[:500]}'

async def fetch_server_status(api_url: str, api_key: str, server_id: str = None, proxy_url: str = None) -> dict:
    """获取服务器状态"""
    server_endpoint, server_id = build_server_endpoint(api_url, server_id)
    resources_url = f"{server_endpoint}/resources"
    proxy = resolve_proxy(proxy_url)

    headers = build_api_headers(api_key, api_url)

    try:
        if proxy:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.get(resources_url, headers=headers, proxy=proxy) as resp:
                    text = await resp.text()
                    if resp.status == 200:
                        data = json.loads(text)
                        return {
                            'success': True,
                            'status': data.get('attributes', {}).get('current_state', 'unknown'),
                            'resources': data.get('attributes', {})
                        }
                    return {'success': False, 'error': format_http_error(resp.status, text, resp.headers)}
        else:
            async with AsyncSession(impersonate="chrome", timeout=15) as session:
                resp = await session.get(resources_url, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    return {
                        'success': True,
                        'status': data.get('attributes', {}).get('current_state', 'unknown'),
                        'resources': data.get('attributes', {})
                    }
                text = resp.text
                return {'success': False, 'error': format_http_error(resp.status_code, text, resp.headers)}
    except asyncio.TimeoutError:
        detail = _xray_log_tail(proxy_url)
        message = 'Request timeout'
        if detail:
            message = f'{message}; Xray log: {detail}'
        return {'success': False, 'error': message}
    except Exception as e:
        detail = _xray_log_tail(proxy_url)
        message = repr(e)
        if detail:
            message = f'{message}; Xray log: {detail}'
        return {'success': False, 'error': message}

async def send_power_action(api_url: str, api_key: str, server_id: str, action: str, proxy_url: str = None) -> dict:
    """发送电源操作"""
    server_endpoint, server_id = build_server_endpoint(api_url, server_id)
    power_url = f"{server_endpoint}/power"
    proxy = resolve_proxy(proxy_url)

    headers = build_api_headers(api_key, api_url)

    try:
        if proxy:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.post(power_url, headers=headers, json={'signal': action}, proxy=proxy) as resp:
                    if resp.status in [200, 204]:
                        return {'success': True}
                    text = await resp.text()
                    return {'success': False, 'error': format_http_error(resp.status, text, resp.headers)}
        else:
            async with AsyncSession(impersonate="chrome", timeout=15) as session:
                resp = await session.post(power_url, headers=headers, json={'signal': action})
                if resp.status_code in [200, 204]:
                    return {'success': True}
                text = resp.text
                return {'success': False, 'error': format_http_error(resp.status_code, text, resp.headers)}
    except Exception as e:
        detail = _xray_log_tail(proxy_url)
        message = repr(e)
        if detail:
            message = f'{message}; Xray log: {detail}'
        return {'success': False, 'error': message}

def add_log(server_id: int, action: str, status: str, message: str):
    """添加日志"""
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO logs (server_id, action, status, message, created_at) VALUES (?, ?, ?, ?, ?)',
        (server_id, action, status, message, now_beijing())
    )
    prune_old_logs(conn)
    conn.commit()
    conn.close()

def prune_old_logs(conn):
    """清理超过保留天数的日志。"""
    cutoff = datetime.now(BEIJING_TZ) - timedelta(days=LOG_RETENTION_DAYS)
    conn.execute('DELETE FROM logs WHERE datetime(created_at) < datetime(?)', (cutoff.isoformat(),))

async def monitor_server(server_id: int):
    """监控单个服务器的任务"""
    logger.info(f"Starting monitor for server {server_id}")
    
    while True:
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute('SELECT * FROM servers WHERE id = ?', (server_id,))
            server = c.fetchone()
            conn.close()
            
            if not server or not server['enabled']:
                logger.info(f"Server {server_id} disabled or removed, stopping monitor")
                break
            
            # 获取代理配置
            proxy_url = server['proxy_url'] if 'proxy_url' in server.keys() else None
            
            # 获取状态
            result = await fetch_server_status(
                server['api_url'], 
                server['api_key'], 
                server['server_id'],
                proxy_url
            )
            
            now = now_beijing()
            
            if result['success']:
                status = result['status']
                
                # 更新状态
                conn = get_db()
                c = conn.cursor()
                c.execute(
                    'UPDATE servers SET last_status = ?, last_check = ? WHERE id = ?',
                    (status, now, server_id)
                )
                conn.commit()
                conn.close()
                
                # 如果offline，自动重启
                if status == 'offline':
                    logger.warning(f"Server {server['name']} is offline, sending restart...")
                    add_log(server_id, 'detect', 'offline', 'Server detected offline')
                    
                    restart_result = await send_power_action(
                        server['api_url'],
                        server['api_key'],
                        server['server_id'],
                        'start',
                        proxy_url
                    )
                    
                    if restart_result['success']:
                        logger.info(f"Restart command sent for {server['name']}")
                        add_log(server_id, 'restart', 'success', 'Restart command sent')
                        
                        # 更新重启计数
                        conn = get_db()
                        c = conn.cursor()
                        c.execute(
                            'UPDATE servers SET restart_count = restart_count + 1 WHERE id = ?',
                            (server_id,)
                        )
                        conn.commit()
                        conn.close()
                    else:
                        logger.error(f"Failed to restart {server['name']}: {restart_result['error']}")
                        add_log(server_id, 'restart', 'failed', restart_result['error'])
            else:
                logger.error(f"Failed to get status for {server['name']}: {result['error']}")
                conn = get_db()
                c = conn.cursor()
                c.execute(
                    'UPDATE servers SET last_status = ?, last_check = ? WHERE id = ?',
                    ('error', now, server_id)
                )
                conn.commit()
                conn.close()
                add_log(server_id, 'check', 'error', result['error'])
            
            # 等待下次检查
            await asyncio.sleep(server['interval'])
            
        except asyncio.CancelledError:
            logger.info(f"Monitor for server {server_id} cancelled")
            break
        except Exception as e:
            logger.error(f"Monitor error for server {server_id}: {e}")
            await asyncio.sleep(10)

def start_monitor(server_id: int):
    """启动服务器监控"""
    if server_id in monitor_tasks:
        monitor_tasks[server_id].cancel()
    
    task = asyncio.create_task(monitor_server(server_id))
    monitor_tasks[server_id] = task
    task.add_done_callback(lambda t, sid=server_id: monitor_tasks.pop(sid, None) if monitor_tasks.get(sid) is t else None)

def stop_monitor(server_id: int):
    """停止服务器监控"""
    if server_id in monitor_tasks:
        monitor_tasks[server_id].cancel()
        del monitor_tasks[server_id]

# ============ Xray 代理管理 ============
# 支持 vless://, vmess://, trojan://, ss:// 格式的代理节点
# 自动启动本地 Xray 进程转换为 SOCKS5/HTTP 代理使用

_xray_procs: Dict[str, tuple] = {}
_next_xray_port = 2080

def _xray_log_tail(proxy_url: Optional[str], limit: int = 800) -> str:
    """读取当前代理节点对应的 Xray 日志尾部。"""
    if not proxy_url:
        return ''
    config_hash = hashlib.md5(proxy_url.strip().encode()).hexdigest()[:12]
    xray_log = os.path.join(tempfile.gettempdir(), f"xray_{config_hash}.log")
    if not os.path.isfile(xray_log):
        return ''
    try:
        with open(xray_log, 'r', errors='replace') as f:
            content = f.read().strip()
        return content[-limit:] if content else ''
    except Exception:
        return ''

def _find_xray() -> Optional[str]:
    """查找 xray 可执行文件"""
    xray_path = shutil.which('xray')
    if xray_path:
        return xray_path
    for p in ['/usr/local/bin/xray', '/opt/xray/xray', './xray']:
        if os.path.isfile(p):
            return p
    return None

def _generate_xray_config(proxy_url: str, socks_port: int = 1080, http_port: int = 1081) -> dict:
    """从 vless/vmess/trojan/ss 链接生成 Xray JSON 配置"""
    proxy_url = html.unescape(proxy_url.strip())
    protocol = proxy_url.split('://')[0]
    content = proxy_url.split('://', 1)[1] if '://' in proxy_url else ''

    if '#' in content:
        content = content.rsplit('#', 1)[0]

    config = {
        "log": {"loglevel": "debug"},
        "inbounds": [
            {"port": http_port, "listen": "127.0.0.1", "protocol": "http"}
        ],
        "outbounds": [],
        "routing": {
            "domainStrategy": "AsIs",
            "rules": [
                {"type": "field", "network": "tcp,udp", "outboundTag": "proxy"}
            ]
        }
    }

    outbound_tag = "proxy"

    if protocol == 'vless':
        uuid, rest = content.split('@', 1)
        if '?' in rest:
            host_port, params_str = rest.split('?', 1)
        else:
            host_port, params_str = rest, ''

        if ':' in host_port:
            address, port = host_port.rsplit(':', 1)
        else:
            address, port = host_port, '443'

        params = parse_qs(params_str)
        security = params.get('security', ['none'])[0]
        network = params.get('type', ['tcp'])[0]
        sni = params.get('sni', [address])[0]
        fp = params.get('fp', ['chrome'])[0]
        flow = params.get('flow', [''])[0]
        pbk = params.get('pbk', [''])[0]
        sid = params.get('sid', [''])[0]
        host = params.get('host', [sni])[0]
        path = unquote(params.get('path', ['/'])[0])

        outbound = {
            "protocol": "vless",
            "settings": {
                "vnext": [{
                    "address": address,
                    "port": int(port),
                    "users": [{"id": uuid, "encryption": "none"}]
                }]
            },
            "streamSettings": {
                "network": network,
                "security": security
            }
        }

        if flow:
            outbound['settings']['vnext'][0]['users'][0]['flow'] = flow

        if security == 'reality':
            reality_settings = {
                "serverName": sni,
                "fingerprint": fp,
                "publicKey": pbk
            }
            if sid:
                reality_settings['shortId'] = sid
            outbound['streamSettings']['realitySettings'] = reality_settings
        elif security == 'tls':
            outbound['streamSettings']['tlsSettings'] = {
                "serverName": sni,
                "fingerprint": fp,
                "allowInsecure": True
            }

        if network == 'ws':
            outbound['streamSettings']['wsSettings'] = {
                "path": path,
                "headers": {"Host": host}
            }
        elif network == 'grpc':
            svc = params.get('serviceName', [''])[0]
            outbound['streamSettings']['grpcSettings'] = {"serviceName": svc}

        outbound['tag'] = 'proxy'
        config['outbounds'].append(outbound)

    elif protocol == 'vmess':
        try:
            padding = 4 - len(content) % 4
            if padding != 4:
                content += '=' * padding
            decoded = base64.b64decode(content).decode('utf-8')
            vm = json.loads(decoded)
        except Exception as e:
            raise ValueError(f'解析 VMess 链接失败: {e}')

        address = vm.get('add', '')
        port = int(vm.get('port', 443))
        uuid = vm.get('id', '')
        aid = int(vm.get('aid', 0))
        network = vm.get('net', 'tcp')
        tls = vm.get('tls', '')
        sni = vm.get('sni', '') or vm.get('host', address)
        host = vm.get('host', address)
        path = vm.get('path', '/')
        fp = vm.get('fp', 'chrome')
        insecure = vm.get('insecure', '1') == '1'

        outbound = {
            "protocol": "vmess",
            "settings": {
                "vnext": [{
                    "address": address,
                    "port": port,
                    "users": [{"id": uuid, "alterId": aid, "security": "auto"}]
                }]
            },
            "streamSettings": {
                "network": network,
                "security": "tls" if tls == 'tls' else "none"
            }
        }

        if tls == 'tls':
            outbound['streamSettings']['tlsSettings'] = {
                "serverName": sni,
                "fingerprint": fp,
                "allowInsecure": insecure
            }

        if network == 'ws':
            outbound['streamSettings']['wsSettings'] = {
                "path": path,
                "headers": {"Host": host}
            }

        outbound['tag'] = 'proxy'
        config['outbounds'].append(outbound)

    elif protocol == 'trojan':
        password, rest = content.split('@', 1)
        if '?' in rest:
            host_port, params_str = rest.split('?', 1)
        else:
            host_port, params_str = rest, ''

        if ':' in host_port:
            address, port = host_port.rsplit(':', 1)
        else:
            address, port = host_port, '443'

        params = parse_qs(params_str)
        sni = params.get('sni', [address])[0]
        network = params.get('type', ['tcp'])[0]
        host = params.get('host', [sni])[0]
        path = unquote(params.get('path', ['/'])[0])
        fp = params.get('fp', ['chrome'])[0]

        outbound = {
            "protocol": "trojan",
            "settings": {
                "servers": [{
                    "address": address,
                    "port": int(port),
                    "password": password
                }]
            },
            "streamSettings": {
                "network": network,
                "security": "tls",
                "tlsSettings": {
                    "serverName": sni,
                    "fingerprint": fp,
                    "allowInsecure": True
                }
            }
        }

        if network == 'ws':
            outbound['streamSettings']['wsSettings'] = {
                "path": path,
                "headers": {"Host": host}
            }

        outbound['tag'] = 'proxy'
        config['outbounds'].append(outbound)

    elif protocol == 'ss':
        plugin_params = {}
        base_content = content
        if '?' in content:
            base_content, query = content.split('?', 1)
            qs = parse_qs(query)
            plugin = qs.get('plugin', [''])[0]
            if plugin:
                plugin = unquote(plugin)
                for item in plugin.split(';'):
                    if '=' in item:
                        k, v = item.split('=', 1)
                        plugin_params[k] = v

        if '@' in base_content:
            encoded, server_part = base_content.split('@', 1)
        else:
            encoded, server_part = base_content, ''

        if ':' in server_part:
            address, port = server_part.split(':', 1)
        else:
            address, port = server_part, '443'

        decoded = base64.b64decode(encoded + '==').decode('utf-8')
        method, password = decoded.split(':', 1)

        outbound = {
            "protocol": "shadowsocks",
            "settings": {
                "servers": [{
                    "address": address,
                    "port": int(port),
                    "method": method,
                    "password": password
                }]
            }
        }

        if plugin_params:
            mode = plugin_params.get('mode', 'websocket')
            is_tls = 'tls' in plugin
            plugin_host = plugin_params.get('host', address)
            plugin_path = plugin_params.get('path', '/')

            outbound['streamSettings'] = {
                "network": "ws",
                "security": "tls" if is_tls else "none",
                "wsSettings": {
                    "path": plugin_path,
                    "headers": {"Host": plugin_host}
                }
            }
            if is_tls:
                outbound['streamSettings']['tlsSettings'] = {
                    "serverName": plugin_host,
                    "allowInsecure": True
                }

        outbound['tag'] = 'proxy'
        config['outbounds'].append(outbound)

    else:
        raise ValueError(f'不支持的代理协议: {protocol}')

    return config


def _start_xray(proxy_url: str) -> Optional[str]:
    """启动 Xray 并返回本地 SOCKS5 代理地址"""
    global _next_xray_port
    xray_path = _find_xray()
    if not xray_path:
        logger.warning("未找到 Xray 二进制文件，无法使用 vless/vmess/trojan/ss 代理")
        return None

    config_hash = hashlib.md5(proxy_url.encode()).hexdigest()[:12]

    if config_hash in _xray_procs:
        proc, port = _xray_procs[config_hash][:2]
        if proc.poll() is None:
            return f"http://127.0.0.1:{port}"
        logger.info(f"Xray 进程已退出 (config: {config_hash})，重新启动")

    socks_port = _next_xray_port
    http_port = _next_xray_port + 1
    _next_xray_port += 2

    try:
        config = _generate_xray_config(proxy_url, socks_port, http_port)
    except Exception as e:
        logger.error(f"解析代理链接失败: {e}")
        return None

    config_file = os.path.join(tempfile.gettempdir(), f"xray_{config_hash}.json")
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)

    out_proto = config['outbounds'][0]['protocol']
    out_addr = 'unknown'
    if 'vnext' in config['outbounds'][0].get('settings', {}):
        out_addr = config['outbounds'][0]['settings']['vnext'][0].get('address', 'unknown')
    elif 'servers' in config['outbounds'][0].get('settings', {}):
        out_addr = config['outbounds'][0]['settings']['servers'][0].get('address', 'unknown')
    logger.info(f"Xray 配置: {out_proto} -> {out_addr}, HTTP 代理端口 {http_port}")

    try:
        xray_log = os.path.join(tempfile.gettempdir(), f"xray_{config_hash}.log")
        with open(xray_log, 'w') as f:
            f.write('')
        log_handle = open(xray_log, 'a')
        proc = subprocess.Popen(
            [xray_path, 'run', '-c', config_file],
            stdout=log_handle,
            stderr=subprocess.STDOUT
        )
        time.sleep(2)
        if proc.poll() is not None:
            with open(xray_log, 'r') as f:
                stderr = f.read()[:500]
            logger.error(f"Xray 启动失败: {stderr}")
            log_handle.close()
            return None
        _xray_procs[config_hash] = (proc, http_port, log_handle)
        logger.info(f"Xray 已启动 (PID {proc.pid}, HTTP 端口 {http_port})")
        return f"http://127.0.0.1:{http_port}"
    except Exception as e:
        logger.error(f"启动 Xray 失败: {e}")
        return None


def stop_all_xray():
    """停止所有 Xray 进程"""
    for config_hash, proc_info in _xray_procs.items():
        proc = proc_info[0]
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            logger.info(f"已停止 Xray (PID {proc.pid})")
        if len(proc_info) > 2:
            try:
                proc_info[2].close()
            except Exception:
                pass
    _xray_procs.clear()


def resolve_proxy(proxy_url: Optional[str]) -> Optional[str]:
    """解析代理地址，返回可用的代理 URL（对 vless/vmess/trojan/ss 自动启动 Xray 转换）"""
    if not proxy_url or not proxy_url.strip():
        return None

    proxy_url = proxy_url.strip()
    proxy_url = html.unescape(proxy_url)
    protocol = proxy_url.split('://')[0] if '://' in proxy_url else ''

    if protocol in ('vless', 'vmess', 'trojan', 'ss'):
        return _start_xray(proxy_url)

    return None


# ============ 认证相关 ============

def set_session_cookie(response, request, token: str):
    """设置会话 Cookie，HTTPS 访问时自动启用 Secure。"""
    response.set_cookie(
        'session',
        token,
        max_age=86400*7,
        httponly=True,
        secure=request.secure,
        samesite='Lax'
    )

def hash_password(password: str) -> str:
    """对密码进行哈希"""
    return hashlib.sha256(password.encode()).hexdigest()

def check_auth(username: str, password: str) -> bool:
    """检查认证"""
    stored_user = get_setting('auth_username')
    stored_pass = get_setting('auth_password')
    
    if not stored_user or not stored_pass:
        return True  # 未设置认证，允许访问
    
    return secrets.compare_digest(username, stored_user) and secrets.compare_digest(hash_password(password), stored_pass)

def is_auth_enabled() -> bool:
    """检查是否启用了认证"""
    return bool(get_setting('auth_username')) and bool(get_setting('auth_password'))

@web.middleware
async def auth_middleware(request, handler):
    """认证中间件"""
    # 登录页面和登录API不需要认证
    if request.path in ['/login', '/api/login', '/api/auth/status']:
        return await handler(request)
    
    # 静态文件中的登录页面
    if request.path.startswith('/static/') and 'login' in request.path:
        return await handler(request)
    
    # 检查是否启用认证
    if not is_auth_enabled():
        return await handler(request)
    
    # 检查 session cookie
    session_token = request.cookies.get('session')
    valid_token = get_setting('session_token')
    
    if session_token and valid_token and secrets.compare_digest(session_token, valid_token):
        return await handler(request)
    
    # 检查 Basic Auth
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Basic '):
        try:
            credentials = base64.b64decode(auth_header[6:]).decode('utf-8')
            username, password = credentials.split(':', 1)
            if check_auth(username, password):
                return await handler(request)
        except:
            pass
    
    # 如果是页面请求，重定向到登录页
    if request.path == '/' or not request.path.startswith('/api/'):
        raise web.HTTPFound('/login')
    
    # API请求返回401
    return web.json_response({'success': False, 'error': '未授权，请登录'}, status=401)

@web.middleware
async def security_headers_middleware(request, handler):
    """添加基础安全响应头，不改变页面现有 inline script/style 行为。"""
    response = await handler(request)
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('X-Frame-Options', 'DENY')
    response.headers.setdefault('Referrer-Policy', 'no-referrer')
    response.headers.setdefault('Permissions-Policy', 'geolocation=(), microphone=(), camera=()')
    return response

# ============ Web API Routes ============

async def index(request):
    """首页"""
    return web.FileResponse('static/index.html')

async def login_page(request):
    """登录页面"""
    return web.FileResponse('static/login.html')

async def api_servers_list(request):
    """获取服务器列表"""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM servers ORDER BY id DESC')
    servers = [dict(row) for row in c.fetchall()]
    conn.close()
    
    # 添加监控状态
    for s in servers:
        s['monitoring'] = s['id'] in monitor_tasks
    
    return web.json_response({'success': True, 'servers': servers})

async def api_server_add(request):
    """添加服务器"""
    try:
        data = await request.json()
        name = data.get('name', '').strip()
        api_url = data.get('api_url', '').strip()
        api_key = data.get('api_key', '').strip()
        server_id = data.get('server_id', '-').strip() or '-'  # 可以为空，从 URL 解析
        interval = int(data.get('interval', 10))
        proxy_url = data.get('proxy_url', '').strip()
        
        if not all([name, api_url, api_key]):
            return web.json_response({'success': False, 'error': '名称、API地址和API Key是必填的'})
        
        if interval < 5:
            interval = 5
        
        conn = get_db()
        c = conn.cursor()
        c.execute(
            'INSERT INTO servers (name, api_url, api_key, server_id, interval, proxy_url) VALUES (?, ?, ?, ?, ?, ?)',
            (name, api_url, api_key, server_id, interval, proxy_url)
        )
        new_id = c.lastrowid
        conn.commit()
        conn.close()
        
        # 自动启动监控
        start_monitor(new_id)
        
        return web.json_response({'success': True, 'id': new_id})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_server_delete(request):
    """删除服务器"""
    try:
        server_id = int(request.match_info['id'])
        
        # 停止监控
        stop_monitor(server_id)
        
        conn = get_db()
        c = conn.cursor()
        c.execute('DELETE FROM logs WHERE server_id = ?', (server_id,))
        c.execute('DELETE FROM servers WHERE id = ?', (server_id,))
        conn.commit()
        conn.close()
        
        return web.json_response({'success': True})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_server_toggle(request):
    """切换服务器监控状态"""
    try:
        server_id = int(request.match_info['id'])
        
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT enabled FROM servers WHERE id = ?', (server_id,))
        row = c.fetchone()
        
        if not row:
            return web.json_response({'success': False, 'error': '服务器不存在'})
        
        new_enabled = 0 if row['enabled'] else 1
        c.execute('UPDATE servers SET enabled = ? WHERE id = ?', (new_enabled, server_id))
        conn.commit()
        conn.close()
        
        if new_enabled:
            start_monitor(server_id)
        else:
            stop_monitor(server_id)
        
        return web.json_response({'success': True, 'enabled': bool(new_enabled)})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_server_update(request):
    """更新服务器配置"""
    try:
        server_id = int(request.match_info['id'])
        data = await request.json()
        
        conn = get_db()
        c = conn.cursor()
        
        updates = []
        params = []
        
        if 'name' in data:
            updates.append('name = ?')
            params.append(data['name'])
        if 'api_url' in data:
            updates.append('api_url = ?')
            params.append(data['api_url'])
        if 'api_key' in data:
            updates.append('api_key = ?')
            params.append(data['api_key'])
        if 'server_id' in data:
            updates.append('server_id = ?')
            params.append(data['server_id'])
        if 'interval' in data:
            interval = max(5, int(data['interval']))
            updates.append('interval = ?')
            params.append(interval)
        if 'proxy_url' in data:
            updates.append('proxy_url = ?')
            params.append(data['proxy_url'])
        
        if updates:
            params.append(server_id)
            c.execute(f"UPDATE servers SET {', '.join(updates)} WHERE id = ?", params)
            conn.commit()
            
            # 重启监控以应用新配置
            c.execute('SELECT enabled FROM servers WHERE id = ?', (server_id,))
            row = c.fetchone()
            if row and row['enabled']:
                start_monitor(server_id)
        
        conn.close()
        return web.json_response({'success': True})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_server_restart(request):
    """手动重启服务器"""
    try:
        server_id = int(request.match_info['id'])
        
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT * FROM servers WHERE id = ?', (server_id,))
        server = c.fetchone()
        conn.close()
        
        if not server:
            return web.json_response({'success': False, 'error': '服务器不存在'})
        
        proxy_url = server['proxy_url'] if 'proxy_url' in server.keys() else None
        result = await send_power_action(
            server['api_url'],
            server['api_key'],
            server['server_id'],
            'restart',
            proxy_url
        )
        
        if result['success']:
            add_log(server_id, 'manual_restart', 'success', 'Manual restart command sent')
        else:
            add_log(server_id, 'manual_restart', 'failed', result['error'])
        
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_server_power(request):
    """服务器电源操作"""
    try:
        server_id = int(request.match_info['id'])
        data = await request.json()
        action = data.get('action', 'start')
        
        if action not in ['start', 'stop', 'restart', 'kill']:
            return web.json_response({'success': False, 'error': '无效的操作'})
        
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT * FROM servers WHERE id = ?', (server_id,))
        server = c.fetchone()
        conn.close()
        
        if not server:
            return web.json_response({'success': False, 'error': '服务器不存在'})
        
        proxy_url = server['proxy_url'] if 'proxy_url' in server.keys() else None
        result = await send_power_action(
            server['api_url'],
            server['api_key'],
            server['server_id'],
            action,
            proxy_url
        )
        
        if result['success']:
            add_log(server_id, f'power_{action}', 'success', f'Power {action} command sent')
        else:
            add_log(server_id, f'power_{action}', 'failed', result['error'])
        
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_server_logs(request):
    """获取服务器日志"""
    try:
        server_id = int(request.match_info['id'])
        limit = int(request.query.get('limit', 50))
        
        conn = get_db()
        c = conn.cursor()
        c.execute(
            'SELECT * FROM logs WHERE server_id = ? ORDER BY id DESC LIMIT ?',
            (server_id, limit)
        )
        logs = [dict(row) for row in c.fetchall()]
        conn.close()
        
        return web.json_response({'success': True, 'logs': logs})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_server_status(request):
    """实时获取服务器状态"""
    try:
        server_id = int(request.match_info['id'])
        
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT * FROM servers WHERE id = ?', (server_id,))
        server = c.fetchone()
        conn.close()
        
        if not server:
            return web.json_response({'success': False, 'error': '服务器不存在'})
        
        proxy_url = server['proxy_url'] if 'proxy_url' in server.keys() else None
        result = await fetch_server_status(
            server['api_url'],
            server['api_key'],
            server['server_id'],
            proxy_url
        )
        
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_all_logs(request):
    """获取所有日志"""
    try:
        limit = int(request.query.get('limit', 100))
        
        conn = get_db()
        c = conn.cursor()
        c.execute('''
            SELECT l.*, s.name as server_name 
            FROM logs l 
            JOIN servers s ON l.server_id = s.id 
            ORDER BY l.id DESC LIMIT ?
        ''', (limit,))
        logs = [dict(row) for row in c.fetchall()]
        conn.close()
        
        return web.json_response({'success': True, 'logs': logs})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_auth_status(request):
    """获取认证状态"""
    return web.json_response({
        'success': True,
        'auth_enabled': is_auth_enabled(),
        'has_username': bool(get_setting('auth_username'))
    })

async def api_login(request):
    """登录"""
    try:
        data = await request.json()
        username = data.get('username', '')
        password = data.get('password', '')
        
        if check_auth(username, password):
            # 生成 session token
            token = secrets.token_hex(32)
            set_setting('session_token', token)
            
            response = web.json_response({'success': True})
            set_session_cookie(response, request, token)
            return response
        else:
            return web.json_response({'success': False, 'error': '用户名或密码错误'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def api_logout(request):
    """登出"""
    set_setting('session_token', '')
    response = web.json_response({'success': True})
    response.del_cookie('session')
    return response

async def api_set_auth(request):
    """设置认证"""
    try:
        data = await request.json()
        username = data.get('username', '').strip()
        password = data.get('password', '').strip()
        
        if username and password:
            set_setting('auth_username', username)
            set_setting('auth_password', hash_password(password))
            # 生成新的 session token
            token = secrets.token_hex(32)
            set_setting('session_token', token)
            
            response = web.json_response({'success': True, 'message': '认证已启用'})
            set_session_cookie(response, request, token)
            return response
        elif not username and not password:
            # 清空认证
            set_setting('auth_username', '')
            set_setting('auth_password', '')
            set_setting('session_token', '')
            response = web.json_response({'success': True, 'message': '认证已禁用'})
            response.del_cookie('session')
            return response
        else:
            return web.json_response({'success': False, 'error': '用户名和密码必须同时设置或同时清空'})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})

async def on_startup(app):
    """启动时恢复所有已启用的监控"""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM servers WHERE enabled = 1')
    servers = c.fetchall()
    conn.close()
    
    for server in servers:
        start_monitor(server['id'])
    
    logger.info(f"Started monitoring for {len(servers)} servers")

async def on_shutdown(app):
    """关闭时停止所有监控和 Xray 代理"""
    tasks = list(monitor_tasks.values())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    monitor_tasks.clear()
    stop_all_xray()
    logger.info("All monitors and proxies stopped")

# ==================== 备份导出/导入 ====================

async def api_export_backup(request):
    """导出备份 - 包含所有服务器配置"""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT name, api_url, api_key, server_id, interval, enabled, proxy_url FROM servers')
        servers = c.fetchall()
        conn.close()
        
        backup_data = {
            'version': '1.0',
            'exported_at': now_beijing(),
            'servers': [
                {
                    'name': s['name'],
                    'api_url': s['api_url'],
                    'api_key': s['api_key'],
                    'server_id': s['server_id'],
                    'interval': s['interval'],
                    'enabled': bool(s['enabled']),
                    'proxy_url': s['proxy_url'] or ''
                }
                for s in servers
            ]
        }
        
        response = web.Response(
            text=json.dumps(backup_data, indent=2, ensure_ascii=False),
            content_type='application/json',
            headers={
                'Content-Disposition': f'attachment; filename="ptero-monitor-backup-{datetime.now(BEIJING_TZ).strftime("%Y%m%d-%H%M%S")}.json"'
            }
        )
        return response
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)

async def api_import_backup(request):
    """导入备份 - 恢复服务器配置"""
    try:
        data = await request.json()
        
        if 'servers' not in data:
            return web.json_response({'success': False, 'error': '无效的备份文件格式'}, status=400)
        
        conn = get_db()
        c = conn.cursor()
        
        imported = 0
        skipped = 0
        
        for server in data['servers']:
            # 检查是否已存在（根据 api_url + server_id 判断）
            c.execute('SELECT id FROM servers WHERE api_url = ? AND server_id = ?', 
                     (server.get('api_url', ''), server.get('server_id', '')))
            existing = c.fetchone()
            
            if existing:
                skipped += 1
                continue
            
            c.execute('''
                INSERT INTO servers (name, api_url, api_key, server_id, interval, enabled, proxy_url)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                server.get('name', '未命名'),
                server.get('api_url', ''),
                server.get('api_key', ''),
                server.get('server_id', ''),
                server.get('interval', 10),
                1 if server.get('enabled', True) else 0,
                server.get('proxy_url', '')
            ))
            
            server_id = c.lastrowid
            imported += 1
            
            # 如果启用了监控，启动监控任务
            if server.get('enabled', True):
                start_monitor(server_id)
        
        conn.commit()
        conn.close()
        
        return web.json_response({
            'success': True,
            'message': f'导入完成: {imported} 个新服务器, {skipped} 个已存在跳过',
            'imported': imported,
            'skipped': skipped
        })
    except json.JSONDecodeError:
        return web.json_response({'success': False, 'error': 'JSON 解析失败'}, status=400)
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)}, status=500)

def create_app():
    """创建应用"""
    app = web.Application(middlewares=[security_headers_middleware, auth_middleware])
    
    # 路由
    app.router.add_get('/', index)
    app.router.add_get('/login', login_page)
    app.router.add_get('/api/servers', api_servers_list)
    app.router.add_post('/api/servers', api_server_add)
    app.router.add_delete('/api/servers/{id}', api_server_delete)
    app.router.add_post('/api/servers/{id}/toggle', api_server_toggle)
    app.router.add_put('/api/servers/{id}', api_server_update)
    app.router.add_post('/api/servers/{id}/restart', api_server_restart)
    app.router.add_post('/api/servers/{id}/power', api_server_power)
    app.router.add_get('/api/servers/{id}/logs', api_server_logs)
    app.router.add_get('/api/servers/{id}/status', api_server_status)
    app.router.add_get('/api/logs', api_all_logs)
    
    # 认证相关
    app.router.add_get('/api/auth/status', api_auth_status)
    app.router.add_post('/api/login', api_login)
    app.router.add_post('/api/logout', api_logout)
    app.router.add_post('/api/auth/set', api_set_auth)
    
    # 备份导出/导入
    app.router.add_get('/api/backup/export', api_export_backup)
    app.router.add_post('/api/backup/import', api_import_backup)
    
    # 静态文件
    app.router.add_static('/static/', 'static')
    
    # 生命周期
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    return app

if __name__ == '__main__':
    init_db()
    app = create_app()
    logger.info(f"Starting Pterodactyl Monitor on port {PORT}")
    web.run_app(app, host='0.0.0.0', port=PORT)
