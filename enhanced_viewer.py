#!/usr/bin/env python3
"""
拡張版データビューア（全企業表示・ページネーション対応）
"""
import http.server
import socketserver
import json
import csv
import os
from urllib.parse import parse_qs, urlparse

class DataHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        
        if parsed.path == '/' or parsed.path == '/index.html':
            self.serve_html()
        elif parsed.path == '/api/data':
            params = parse_qs(parsed.query)
            page = int(params.get('page', ['1'])[0])
            per_page = int(params.get('per_page', ['100'])[0])
            search = params.get('search', [''])[0]
            contact_only = params.get('contact_only', ['false'])[0] == 'true'
            self.serve_data(page, per_page, search, contact_only)
        elif parsed.path.startswith('/download'):
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
            max-width: 1600px;
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
            flex-wrap: wrap;
        }
        .search-box {
            flex: 1;
            min-width: 300px;
            padding: 12px 20px;
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            font-size: 16px;
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
        }
        .filter-btn.active {
            background: #805ad5;
        }
        .download-btn {
            background: #48bb78;
            color: white;
        }
        .download-btn:hover {
            background: #38a169;
        }
        .view-options {
            display: flex;
            gap: 10px;
            align-items: center;
        }
        .view-btn {
            padding: 8px 16px;
            background: white;
            border: 2px solid #667eea;
            color: #667eea;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            transition: all 0.3s;
        }
        .view-btn.active {
            background: #667eea;
            color: white;
        }
        .per-page-select {
            padding: 8px 12px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 14px;
            cursor: pointer;
        }
        .table-container {
            padding: 30px 40px;
            overflow-x: auto;
            min-height: 500px;
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
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            position: sticky;
            top: 0;
            z-index: 10;
        }
        td {
            padding: 12px 15px;
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
        .contact-btn {
            background: #667eea;
            color: white;
            padding: 6px 12px;
            border-radius: 6px;
            text-decoration: none;
            font-size: 13px;
            display: inline-block;
        }
        .contact-btn:hover {
            background: #5a67d8;
        }
        .no-contact {
            color: #cbd5e0;
            font-style: italic;
            font-size: 13px;
        }
        .badge {
            display: inline-block;
            padding: 3px 8px;
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
        .pagination {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 10px;
            padding: 30px;
            background: #f8f9fa;
            border-top: 1px solid #e0e0e0;
        }
        .page-btn {
            padding: 8px 16px;
            background: white;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.3s;
        }
        .page-btn:hover:not(:disabled) {
            background: #667eea;
            color: white;
            border-color: #667eea;
        }
        .page-btn.active {
            background: #667eea;
            color: white;
            border-color: #667eea;
        }
        .page-btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        .page-info {
            padding: 8px 16px;
            color: #4a5568;
            font-size: 14px;
        }
        .status-bar {
            padding: 15px 40px;
            background: #edf2f7;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 14px;
            color: #4a5568;
        }
        .warning-banner {
            background: #fff5f5;
            border-left: 4px solid #fc8181;
            padding: 15px 20px;
            margin: 20px 40px;
            border-radius: 8px;
            display: none;
        }
        .warning-banner h4 {
            color: #c53030;
            margin-bottom: 5px;
        }
        .warning-banner p {
            color: #742a2a;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏢 優良WEB 企業データベース</h1>
            <div class="stats">
                <div class="stat">
                    <span class="stat-number" id="totalCount">10,021</span>
                    <div class="stat-label">総企業数</div>
                </div>
                <div class="stat">
                    <span class="stat-number" id="contactCount">8,544</span>
                    <div class="stat-label">問い合わせURL取得</div>
                </div>
                <div class="stat">
                    <span class="stat-number">85.3%</span>
                    <div class="stat-label">取得成功率</div>
                </div>
            </div>
        </div>
        
        <div class="warning-banner" id="warningBanner">
            <h4>⚠️ パフォーマンス警告</h4>
            <p>1000件以上の表示は処理に時間がかかる場合があります。必要に応じて表示件数を調整してください。</p>
        </div>
        
        <div class="controls">
            <input type="text" class="search-box" id="searchBox" placeholder="企業名、URL、住所で検索..." onkeyup="handleSearch()">
            <button class="btn filter-btn" id="contactFilter" onclick="toggleContactFilter()">
                📧 問い合わせ有のみ
            </button>
            <div class="view-options">
                <span style="color: #4a5568; font-size: 14px;">表示件数:</span>
                <select class="per-page-select" id="perPageSelect" onchange="changePerPage()">
                    <option value="50">50件</option>
                    <option value="100" selected>100件</option>
                    <option value="200">200件</option>
                    <option value="500">500件</option>
                    <option value="1000">1000件</option>
                    <option value="all">全て表示</option>
                </select>
            </div>
            <a href="/download" class="btn download-btn">
                📥 CSVダウンロード
            </a>
        </div>
        
        <div class="status-bar">
            <div id="showingInfo">表示中: 1-100件 / 全10,021件</div>
            <div id="filterInfo">フィルター: なし</div>
        </div>
        
        <div class="table-container">
            <div class="loading" id="loading">データを読み込み中...</div>
            <table id="dataTable" style="display: none;">
                <thead>
                    <tr>
                        <th style="width: 50px;">No.</th>
                        <th>企業名</th>
                        <th>公式サイト</th>
                        <th>問い合わせ</th>
                        <th>住所</th>
                        <th>地域</th>
                        <th>価格</th>
                        <th>業種</th>
                    </tr>
                </thead>
                <tbody id="tableBody">
                </tbody>
            </table>
        </div>
        
        <div class="pagination" id="pagination" style="display: none;">
            <button class="page-btn" onclick="changePage('first')">≪ 最初</button>
            <button class="page-btn" onclick="changePage('prev')">＜ 前へ</button>
            <span class="page-info" id="pageInfo">1 / 10</span>
            <button class="page-btn" onclick="changePage('next')">次へ ＞</button>
            <button class="page-btn" onclick="changePage('last')">最後 ≫</button>
        </div>
    </div>
    
    <script>
        let currentPage = 1;
        let perPage = 100;
        let totalPages = 1;
        let totalItems = 0;
        let searchTerm = '';
        let contactOnly = false;
        let isLoading = false;
        
        // 初期データ読み込み
        loadData();
        
        function loadData() {
            if (isLoading) return;
            isLoading = true;
            
            document.getElementById('loading').style.display = 'block';
            document.getElementById('dataTable').style.display = 'none';
            
            const actualPerPage = perPage === 'all' ? 10021 : perPage;
            
            fetch(`/api/data?page=${currentPage}&per_page=${actualPerPage}&search=${encodeURIComponent(searchTerm)}&contact_only=${contactOnly}`)
                .then(response => response.json())
                .then(data => {
                    renderTable(data.items);
                    updatePagination(data);
                    updateStatus(data);
                    document.getElementById('loading').style.display = 'none';
                    document.getElementById('dataTable').style.display = 'table';
                    
                    // パフォーマンス警告
                    if (actualPerPage >= 1000) {
                        document.getElementById('warningBanner').style.display = 'block';
                    } else {
                        document.getElementById('warningBanner').style.display = 'none';
                    }
                    
                    isLoading = false;
                })
                .catch(error => {
                    document.getElementById('loading').innerHTML = 'エラー: ' + error;
                    isLoading = false;
                });
        }
        
        function renderTable(data) {
            const tbody = document.getElementById('tableBody');
            tbody.innerHTML = '';
            
            const startIndex = (currentPage - 1) * (perPage === 'all' ? 10021 : perPage);
            
            data.forEach((row, index) => {
                const tr = document.createElement('tr');
                
                // カテゴリバッジ作成
                let areaHtml = '';
                if (row.area_categories) {
                    row.area_categories.split('｜').slice(0, 2).forEach(cat => {
                        if (cat) areaHtml += `<span class="badge badge-area">${cat}</span>`;
                    });
                }
                
                let priceHtml = '';
                if (row.price_categories) {
                    row.price_categories.split('｜').slice(0, 1).forEach(cat => {
                        if (cat) priceHtml += `<span class="badge badge-price">${cat}</span>`;
                    });
                }
                
                let industryHtml = '';
                if (row.industry_categories) {
                    row.industry_categories.split('｜').slice(0, 2).forEach(cat => {
                        if (cat) industryHtml += `<span class="badge badge-industry">${cat}</span>`;
                    });
                }
                
                tr.innerHTML = `
                    <td style="color: #a0aec0; text-align: center;">${startIndex + index + 1}</td>
                    <td class="company-name">${row.company_name}</td>
                    <td class="url-cell">
                        <a href="${row.official_site_url}" target="_blank" style="color: #4a5568;">
                            ${row.official_site_url}
                        </a>
                    </td>
                    <td>
                        ${row.contact_url ? 
                            `<a href="${row.contact_url}" target="_blank" class="contact-btn">📧 問合せ</a>` : 
                            '<span class="no-contact">-</span>'}
                    </td>
                    <td>${row.address || '-'}</td>
                    <td>${areaHtml || '-'}</td>
                    <td>${priceHtml || '-'}</td>
                    <td>${industryHtml || '-'}</td>
                `;
                
                tbody.appendChild(tr);
            });
        }
        
        function updatePagination(data) {
            totalItems = data.total;
            totalPages = data.total_pages;
            currentPage = data.page;
            
            document.getElementById('pageInfo').textContent = `${currentPage} / ${totalPages}`;
            
            // ページネーション表示/非表示
            if (perPage === 'all' || totalPages <= 1) {
                document.getElementById('pagination').style.display = 'none';
            } else {
                document.getElementById('pagination').style.display = 'flex';
            }
        }
        
        function updateStatus(data) {
            const start = (data.page - 1) * data.per_page + 1;
            const end = Math.min(start + data.items.length - 1, data.total);
            
            document.getElementById('showingInfo').textContent = 
                `表示中: ${start}-${end}件 / 全${data.total}件`;
            
            document.getElementById('totalCount').textContent = data.total.toLocaleString();
            document.getElementById('contactCount').textContent = data.with_contact.toLocaleString();
            
            // フィルター情報
            let filterText = [];
            if (searchTerm) filterText.push(`検索: "${searchTerm}"`);
            if (contactOnly) filterText.push('問い合わせ有のみ');
            
            document.getElementById('filterInfo').textContent = 
                filterText.length > 0 ? `フィルター: ${filterText.join(', ')}` : 'フィルター: なし';
        }
        
        function changePage(action) {
            if (isLoading) return;
            
            switch(action) {
                case 'first':
                    currentPage = 1;
                    break;
                case 'prev':
                    if (currentPage > 1) currentPage--;
                    break;
                case 'next':
                    if (currentPage < totalPages) currentPage++;
                    break;
                case 'last':
                    currentPage = totalPages;
                    break;
            }
            loadData();
        }
        
        function changePerPage() {
            const select = document.getElementById('perPageSelect');
            perPage = select.value === 'all' ? 'all' : parseInt(select.value);
            currentPage = 1;
            loadData();
        }
        
        function handleSearch() {
            clearTimeout(window.searchTimer);
            window.searchTimer = setTimeout(() => {
                searchTerm = document.getElementById('searchBox').value;
                currentPage = 1;
                loadData();
            }, 500); // 0.5秒のデバウンス
        }
        
        function toggleContactFilter() {
            contactOnly = !contactOnly;
            const btn = document.getElementById('contactFilter');
            if (contactOnly) {
                btn.classList.add('active');
                btn.textContent = '🔍 全て表示';
            } else {
                btn.classList.remove('active');
                btn.textContent = '📧 問い合わせ有のみ';
            }
            currentPage = 1;
            loadData();
        }
    </script>
</body>
</html>
'''
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html.encode('utf-8'))
    
    def serve_data(self, page=1, per_page=100, search='', contact_only=False):
        """JSONデータを提供（ページネーション対応）"""
        csv_file = 'yuryoweb_complete_with_contact_20250814_005638.csv'
        
        if not os.path.exists(csv_file):
            csv_file = 'yuryoweb_final_complete_20250814_001100.csv'
        
        try:
            # 全データ読み込み
            all_data = []
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # フィルタリング
                    if search:
                        search_lower = search.lower()
                        if not any([
                            search_lower in (row.get('company_name', '') or '').lower(),
                            search_lower in (row.get('official_site_url', '') or '').lower(),
                            search_lower in (row.get('address', '') or '').lower()
                        ]):
                            continue
                    
                    if contact_only and not row.get('contact_url'):
                        continue
                    
                    all_data.append(row)
            
            # ページネーション計算
            total = len(all_data)
            total_pages = (total + per_page - 1) // per_page if per_page < total else 1
            
            # 該当ページのデータ取得
            start_idx = (page - 1) * per_page
            end_idx = min(start_idx + per_page, total)
            page_data = all_data[start_idx:end_idx]
            
            # 統計計算
            with_contact = sum(1 for d in all_data if d.get('contact_url'))
            
            response = {
                'items': page_data,
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': total_pages,
                'with_contact': with_contact
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response, ensure_ascii=False).encode('utf-8'))
            
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
            self.send_header('Content-type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', 'attachment; filename="yuryoweb_data.csv"')
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404, "File not found")

def main():
    PORT = 3000  # ポート3000に変更
    
    print("="*60)
    print("🌐 優良WEB データビューア（拡張版）")
    print("="*60)
    print(f"\n✅ サーバー起動完了！")
    print(f"\n以下のURLでアクセスしてください:")
    print(f"\n  http://localhost:{PORT}")
    print(f"\n機能:")
    print("  • 全10,021社の表示対応")
    print("  • ページネーション")
    print("  • リアルタイム検索")
    print("  • 問い合わせURLフィルター")
    print("  • CSVダウンロード")
    print(f"\nCtrl+C で終了")
    print("="*60)
    
    # ポート使用中のエラー対策
    socketserver.TCPServer.allow_reuse_address = True
    
    with socketserver.TCPServer(("", PORT), DataHandler) as httpd:
        httpd.serve_forever()

if __name__ == '__main__':
    main()