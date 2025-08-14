#!/usr/bin/env python3
"""
シンプルなデータビューア（標準ライブラリのみ使用）
"""
import http.server
import socketserver
import json
import csv
import os
from urllib.parse import parse_qs, urlparse

class DataHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/' or self.path == '/index.html':
            self.serve_html()
        elif self.path == '/api/data':
            self.serve_data()
        elif self.path.startswith('/download'):
            self.serve_download()
        else:
            super().do_GET()
    
    def serve_html(self):
        """HTMLページを提供"""
        html = '''
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>優良WEB データビューア</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        h1 {
            font-size: 2.5em;
            margin-bottom: 20px;
            font-weight: 300;
            letter-spacing: -1px;
        }
        .stats {
            display: flex;
            justify-content: center;
            gap: 60px;
            margin-top: 30px;
            flex-wrap: wrap;
        }
        .stat {
            text-align: center;
        }
        .stat-number {
            font-size: 3em;
            font-weight: bold;
            display: block;
        }
        .stat-label {
            font-size: 1em;
            opacity: 0.9;
            margin-top: 5px;
        }
        .controls {
            padding: 25px 40px;
            background: #f8f9fa;
            display: flex;
            gap: 20px;
            align-items: center;
            border-bottom: 1px solid #e0e0e0;
        }
        .search-box {
            flex: 1;
            padding: 12px 20px;
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            font-size: 16px;
            transition: border-color 0.3s;
        }
        .search-box:focus {
            outline: none;
            border-color: #667eea;
        }
        .btn {
            padding: 12px 24px;
            border: none;
            border-radius: 10px;
            cursor: pointer;
            font-size: 15px;
            font-weight: 600;
            transition: all 0.3s;
            text-decoration: none;
            display: inline-block;
        }
        .filter-btn {
            background: #667eea;
            color: white;
        }
        .filter-btn:hover {
            background: #5a67d8;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
        }
        .download-btn {
            background: #48bb78;
            color: white;
        }
        .download-btn:hover {
            background: #38a169;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(72, 187, 120, 0.4);
        }
        .table-container {
            padding: 40px;
            overflow-x: auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th {
            background: #f7fafc;
            padding: 15px;
            text-align: left;
            font-weight: 600;
            color: #4a5568;
            border-bottom: 2px solid #e2e8f0;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        td {
            padding: 15px;
            border-bottom: 1px solid #e2e8f0;
            font-size: 14px;
        }
        tr:hover {
            background: #f7fafc;
        }
        .company-name {
            font-weight: 600;
            color: #2d3748;
        }
        .url-cell {
            max-width: 250px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .url-link {
            color: #4a5568;
            text-decoration: none;
        }
        .url-link:hover {
            color: #667eea;
            text-decoration: underline;
        }
        .contact-btn {
            background: #667eea;
            color: white;
            padding: 6px 12px;
            border-radius: 6px;
            text-decoration: none;
            font-size: 13px;
            display: inline-block;
            transition: all 0.2s;
        }
        .contact-btn:hover {
            background: #5a67d8;
            transform: scale(1.05);
        }
        .no-contact {
            color: #cbd5e0;
            font-style: italic;
            font-size: 13px;
        }
        .badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 600;
            margin: 2px;
            white-space: nowrap;
        }
        .badge-area { background: #bee3f8; color: #2c5282; }
        .badge-price { background: #c6f6d5; color: #22543d; }
        .badge-feature { background: #fed7d7; color: #742a2a; }
        .badge-industry { background: #feebc8; color: #7c2d12; }
        .loading {
            text-align: center;
            padding: 60px;
            color: #718096;
            font-size: 18px;
        }
        .info-box {
            background: #edf2f7;
            border-left: 4px solid #667eea;
            padding: 20px;
            margin: 20px 40px;
            border-radius: 8px;
        }
        .info-box h3 {
            color: #2d3748;
            margin-bottom: 10px;
        }
        .info-box p {
            color: #4a5568;
            line-height: 1.6;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏢 優良WEB 企業データベース</h1>
            <div class="stats">
                <div class="stat">
                    <span class="stat-number">10,021</span>
                    <div class="stat-label">総企業数</div>
                </div>
                <div class="stat">
                    <span class="stat-number">8,544</span>
                    <div class="stat-label">問い合わせURL取得</div>
                </div>
                <div class="stat">
                    <span class="stat-number">85.3%</span>
                    <div class="stat-label">取得成功率</div>
                </div>
            </div>
        </div>
        
        <div class="info-box">
            <h3>📊 データ概要</h3>
            <p>優良WEBから取得した10,021社の企業データです。地域・価格・特徴・業種の各カテゴリ情報と、85%以上の企業で問い合わせURLを自動取得しています。</p>
        </div>
        
        <div class="controls">
            <input type="text" class="search-box" id="searchBox" placeholder="企業名、URL、住所で検索..." onkeyup="filterTable()">
            <button class="btn filter-btn" onclick="toggleContactFilter()">
                <span id="filterText">📧 問い合わせ有のみ</span>
            </button>
            <a href="/download" class="btn download-btn">
                📥 CSVダウンロード
            </a>
        </div>
        
        <div class="table-container">
            <div class="loading" id="loading">データを読み込み中...</div>
            <table id="dataTable" style="display: none;">
                <thead>
                    <tr>
                        <th>企業名</th>
                        <th>公式サイト</th>
                        <th>問い合わせ</th>
                        <th>住所</th>
                        <th>カテゴリ</th>
                    </tr>
                </thead>
                <tbody id="tableBody">
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        let allData = [];
        let showContactOnly = false;
        
        // データ読み込み
        fetch('/api/data')
            .then(response => response.json())
            .then(data => {
                allData = data;
                renderTable(data.slice(0, 100));
                document.getElementById('loading').style.display = 'none';
                document.getElementById('dataTable').style.display = 'table';
            })
            .catch(error => {
                document.getElementById('loading').innerHTML = 'データの読み込みに失敗しました: ' + error;
            });
        
        function renderTable(data) {
            const tbody = document.getElementById('tableBody');
            tbody.innerHTML = '';
            
            data.forEach(row => {
                const tr = document.createElement('tr');
                tr.dataset.hasContact = row.contact_url ? 'yes' : 'no';
                
                // カテゴリバッジ作成
                let categories = '';
                if (row.area_categories) {
                    row.area_categories.split('｜').slice(0, 2).forEach(cat => {
                        if (cat) categories += `<span class="badge badge-area">${cat}</span>`;
                    });
                }
                if (row.price_categories) {
                    row.price_categories.split('｜').slice(0, 1).forEach(cat => {
                        if (cat) categories += `<span class="badge badge-price">${cat}</span>`;
                    });
                }
                if (row.industry_categories) {
                    row.industry_categories.split('｜').slice(0, 2).forEach(cat => {
                        if (cat) categories += `<span class="badge badge-industry">${cat}</span>`;
                    });
                }
                
                tr.innerHTML = `
                    <td class="company-name">${row.company_name}</td>
                    <td class="url-cell">
                        <a href="${row.official_site_url}" target="_blank" class="url-link">
                            ${row.official_site_url}
                        </a>
                    </td>
                    <td>
                        ${row.contact_url ? 
                            `<a href="${row.contact_url}" target="_blank" class="contact-btn">📧 問い合わせ</a>` : 
                            '<span class="no-contact">-</span>'}
                    </td>
                    <td>${row.address || '-'}</td>
                    <td>${categories || '-'}</td>
                `;
                
                tbody.appendChild(tr);
            });
        }
        
        function filterTable() {
            const searchTerm = document.getElementById('searchBox').value.toLowerCase();
            const filteredData = allData.filter(row => {
                const matchesSearch = 
                    row.company_name.toLowerCase().includes(searchTerm) ||
                    (row.official_site_url || '').toLowerCase().includes(searchTerm) ||
                    (row.address || '').toLowerCase().includes(searchTerm);
                
                const matchesFilter = !showContactOnly || row.contact_url;
                
                return matchesSearch && matchesFilter;
            });
            
            renderTable(filteredData.slice(0, 100));
        }
        
        function toggleContactFilter() {
            showContactOnly = !showContactOnly;
            document.getElementById('filterText').textContent = 
                showContactOnly ? '🔍 全て表示' : '📧 問い合わせ有のみ';
            filterTable();
        }
    </script>
</body>
</html>
'''
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html.encode('utf-8'))
    
    def serve_data(self):
        """JSONデータを提供"""
        csv_file = 'yuryoweb_complete_with_contact_20250814_005638.csv'
        
        if not os.path.exists(csv_file):
            csv_file = 'yuryoweb_final_complete_20250814_001100.csv'
        
        try:
            data = []
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data.append(row)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps(data[:500], ensure_ascii=False).encode('utf-8'))
        except Exception as e:
            self.send_error(500, str(e))
    
    def serve_download(self):
        """CSVダウンロード"""
        csv_file = 'yuryoweb_complete_with_contact_20250814_005638.csv'
        
        if not os.path.exists(csv_file):
            csv_file = 'yuryoweb_final_complete_20250814_001100.csv'
        
        if os.path.exists(csv_file):
            with open(csv_file, 'rb') as f:
                content = f.read()
            
            self.send_response(200)
            self.send_header('Content-type', 'text/csv')
            self.send_header('Content-Disposition', 'attachment; filename="yuryoweb_data.csv"')
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404, "File not found")

def main():
    PORT = 8080
    
    print("="*60)
    print("🌐 優良WEB データビューア")
    print("="*60)
    print(f"\n✅ サーバー起動完了！")
    print(f"\n以下のURLでアクセスしてください:")
    print(f"\n  http://localhost:{PORT}")
    print(f"\nCtrl+C で終了")
    print("="*60)
    
    with socketserver.TCPServer(("", PORT), DataHandler) as httpd:
        httpd.serve_forever()

if __name__ == '__main__':
    main()