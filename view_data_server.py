#!/usr/bin/env python3
"""
データ閲覧用簡易Webサーバー
"""
from flask import Flask, render_template_string, send_file, jsonify
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

# HTMLテンプレート
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>優良WEB データビューア</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
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
            padding: 30px;
            text-align: center;
        }
        h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        .stats {
            display: flex;
            justify-content: center;
            gap: 40px;
            margin-top: 20px;
            flex-wrap: wrap;
        }
        .stat {
            text-align: center;
        }
        .stat-number {
            font-size: 2em;
            font-weight: bold;
        }
        .stat-label {
            font-size: 0.9em;
            opacity: 0.9;
        }
        .controls {
            padding: 20px 30px;
            background: #f8f9fa;
            display: flex;
            gap: 15px;
            align-items: center;
            flex-wrap: wrap;
        }
        .search-box {
            flex: 1;
            min-width: 300px;
            padding: 10px 15px;
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            font-size: 16px;
        }
        .filter-btn {
            padding: 10px 20px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 10px;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.3s;
        }
        .filter-btn:hover {
            background: #5a67d8;
            transform: translateY(-2px);
        }
        .download-btn {
            padding: 10px 20px;
            background: #48bb78;
            color: white;
            border: none;
            border-radius: 10px;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.3s;
        }
        .download-btn:hover {
            background: #38a169;
            transform: translateY(-2px);
        }
        .table-container {
            padding: 30px;
            overflow-x: auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th {
            background: #f7fafc;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #4a5568;
            border-bottom: 2px solid #e2e8f0;
            position: sticky;
            top: 0;
        }
        td {
            padding: 12px;
            border-bottom: 1px solid #e2e8f0;
        }
        tr:hover {
            background: #f7fafc;
        }
        .url-cell {
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .contact-url {
            color: #667eea;
            text-decoration: none;
        }
        .contact-url:hover {
            text-decoration: underline;
        }
        .badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 600;
            margin: 2px;
        }
        .badge-area { background: #bee3f8; color: #2c5282; }
        .badge-price { background: #c6f6d5; color: #22543d; }
        .badge-feature { background: #fed7d7; color: #742a2a; }
        .badge-industry { background: #feebc8; color: #7c2d12; }
        .no-contact { color: #a0aec0; font-style: italic; }
        .loading {
            text-align: center;
            padding: 50px;
            color: #718096;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏢 優良WEB 企業データベース</h1>
            <div class="stats">
                <div class="stat">
                    <div class="stat-number">{{ total_companies }}</div>
                    <div class="stat-label">総企業数</div>
                </div>
                <div class="stat">
                    <div class="stat-number">{{ with_contact }}</div>
                    <div class="stat-label">問い合わせURL取得</div>
                </div>
                <div class="stat">
                    <div class="stat-number">{{ contact_rate }}%</div>
                    <div class="stat-label">取得率</div>
                </div>
            </div>
        </div>
        
        <div class="controls">
            <input type="text" class="search-box" id="searchBox" placeholder="企業名、URL、住所で検索..." onkeyup="filterTable()">
            <button class="filter-btn" onclick="toggleContactFilter()">
                <span id="filterText">問い合わせURL有りのみ</span>
            </button>
            <a href="/download" class="download-btn" style="text-decoration: none;">
                📥 CSVダウンロード
            </a>
        </div>
        
        <div class="table-container">
            <table id="dataTable">
                <thead>
                    <tr>
                        <th>企業名</th>
                        <th>公式サイト</th>
                        <th>問い合わせURL</th>
                        <th>住所</th>
                        <th>地域</th>
                        <th>価格</th>
                        <th>特徴</th>
                        <th>業種</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in companies[:100] %}
                    <tr data-has-contact="{{ 'yes' if row.contact_url else 'no' }}">
                        <td><strong>{{ row.company_name }}</strong></td>
                        <td class="url-cell">
                            <a href="{{ row.official_site_url }}" target="_blank" style="color: #4a5568;">
                                {{ row.official_site_url }}
                            </a>
                        </td>
                        <td>
                            {% if row.contact_url %}
                                <a href="{{ row.contact_url }}" target="_blank" class="contact-url">
                                    📧 問い合わせ
                                </a>
                            {% else %}
                                <span class="no-contact">-</span>
                            {% endif %}
                        </td>
                        <td>{{ row.address or '-' }}</td>
                        <td>
                            {% for cat in (row.area_categories or '').split('｜') if cat %}
                                <span class="badge badge-area">{{ cat }}</span>
                            {% endfor %}
                        </td>
                        <td>
                            {% for cat in (row.price_categories or '').split('｜')[:2] if cat %}
                                <span class="badge badge-price">{{ cat }}</span>
                            {% endfor %}
                        </td>
                        <td>
                            {% for cat in (row.feature_categories or '').split('｜')[:2] if cat %}
                                <span class="badge badge-feature">{{ cat }}</span>
                            {% endfor %}
                        </td>
                        <td>
                            {% for cat in (row.industry_categories or '').split('｜')[:3] if cat %}
                                <span class="badge badge-industry">{{ cat }}</span>
                            {% endfor %}
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            <div style="text-align: center; padding: 20px; color: #718096;">
                最初の100件を表示中 / 全{{ total_companies }}件
            </div>
        </div>
    </div>
    
    <script>
        let showContactOnly = false;
        
        function filterTable() {
            const searchTerm = document.getElementById('searchBox').value.toLowerCase();
            const rows = document.querySelectorAll('#dataTable tbody tr');
            
            rows.forEach(row => {
                const text = row.textContent.toLowerCase();
                const hasContact = row.dataset.hasContact === 'yes';
                
                const matchesSearch = text.includes(searchTerm);
                const matchesFilter = !showContactOnly || hasContact;
                
                row.style.display = (matchesSearch && matchesFilter) ? '' : 'none';
            });
        }
        
        function toggleContactFilter() {
            showContactOnly = !showContactOnly;
            document.getElementById('filterText').textContent = 
                showContactOnly ? '全て表示' : '問い合わせURL有りのみ';
            filterTable();
        }
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    """メインページ"""
    # 最新のCSVファイルを読み込み
    csv_file = 'yuryoweb_complete_with_contact_20250814_005638.csv'
    
    if not os.path.exists(csv_file):
        # 代替ファイル
        csv_file = 'yuryoweb_final_complete_20250814_001100.csv'
    
    try:
        df = pd.read_csv(csv_file)
        
        # 統計情報
        total = len(df)
        with_contact = df['contact_url'].notna().sum() if 'contact_url' in df.columns else 0
        rate = round(with_contact / total * 100, 1) if total > 0 else 0
        
        # DataFrameをdictに変換
        companies = df.fillna('').to_dict('records')
        
        return render_template_string(
            HTML_TEMPLATE,
            companies=companies,
            total_companies=f"{total:,}",
            with_contact=f"{with_contact:,}",
            contact_rate=rate
        )
    except Exception as e:
        return f"エラー: {e}", 500

@app.route('/download')
def download():
    """CSVダウンロード"""
    csv_file = 'yuryoweb_complete_with_contact_20250814_005638.csv'
    
    if not os.path.exists(csv_file):
        csv_file = 'yuryoweb_final_complete_20250814_001100.csv'
    
    if os.path.exists(csv_file):
        return send_file(csv_file, as_attachment=True, download_name='yuryoweb_data.csv')
    else:
        return "ファイルが見つかりません", 404

@app.route('/api/stats')
def api_stats():
    """統計情報API"""
    csv_file = 'yuryoweb_complete_with_contact_20250814_005638.csv'
    
    if not os.path.exists(csv_file):
        csv_file = 'yuryoweb_final_complete_20250814_001100.csv'
    
    try:
        df = pd.read_csv(csv_file)
        
        stats = {
            'total_companies': len(df),
            'with_contact': df['contact_url'].notna().sum() if 'contact_url' in df.columns else 0,
            'with_area': df['area_categories'].notna().sum() if 'area_categories' in df.columns else 0,
            'with_price': df['price_categories'].notna().sum() if 'price_categories' in df.columns else 0,
            'with_feature': df['feature_categories'].notna().sum() if 'feature_categories' in df.columns else 0,
            'with_industry': df['industry_categories'].notna().sum() if 'industry_categories' in df.columns else 0,
        }
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("="*60)
    print("優良WEB データビューアー")
    print("="*60)
    print("\n起動中...")
    print("\n✅ サーバー起動完了！")
    print("\n以下のURLでアクセスしてください:")
    print("\n  http://localhost:5000")
    print("\nCtrl+C で終了")
    print("="*60)
    
    app.run(debug=False, port=5000, host='0.0.0.0')