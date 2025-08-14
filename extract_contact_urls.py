#!/usr/bin/env python3
"""
企業サイトから問い合わせフォームURLを抽出
"""
import csv
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import time
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 問い合わせ関連のキーワード
CONTACT_KEYWORDS = [
    'contact', 'inquiry', 'form', '問い合わせ', 'お問い合わせ', '問合せ', '問合わせ',
    'toiawase', 'otoiawase', 'mail', 'メール', 'フォーム', '相談', 'soudan',
    '見積', 'estimate', 'mitsumori', '資料請求', 'request', '申し込み', 'apply',
    'support', 'サポート', 'ご相談', 'ご質問', 'question'
]

# 除外するキーワード（プライバシーポリシーなど）
EXCLUDE_KEYWORDS = [
    'privacy', 'policy', 'terms', 'プライバシー', '規約', '利用規約',
    'sitemap', 'サイトマップ', 'recruit', '採用', 'career', 'login', 'ログイン'
]

class ContactExtractor:
    def __init__(self, timeout=10, max_workers=5):
        self.timeout = timeout
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.lock = threading.Lock()
        self.results = []
        
    def is_contact_url(self, url, text=''):
        """URLや付随テキストが問い合わせ関連か判定"""
        url_lower = url.lower()
        text_lower = text.lower()
        
        # 除外キーワードチェック
        for keyword in EXCLUDE_KEYWORDS:
            if keyword in url_lower or keyword in text_lower:
                return False
        
        # 問い合わせキーワードチェック
        for keyword in CONTACT_KEYWORDS:
            if keyword in url_lower or keyword in text_lower:
                return True
        
        return False
    
    def extract_from_site(self, company_name, site_url):
        """1つのサイトから問い合わせURLを抽出"""
        try:
            # URLの正規化
            if not site_url.startswith(('http://', 'https://')):
                site_url = 'https://' + site_url
            
            # サイトにアクセス
            response = self.session.get(site_url, timeout=self.timeout)
            response.raise_for_status()
            
            # HTMLをパース
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 問い合わせ関連のリンクを収集
            contact_urls = set()
            
            # 全てのリンクを確認
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # 相対URLを絶対URLに変換
                full_url = urljoin(site_url, href)
                
                # 同じドメイン内のみ
                if urlparse(full_url).netloc == urlparse(site_url).netloc:
                    if self.is_contact_url(full_url, text):
                        contact_urls.add(full_url)
            
            # メタ情報やナビゲーションからも探す
            nav_elements = soup.find_all(['nav', 'header', 'footer'])
            for nav in nav_elements:
                links = nav.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    full_url = urljoin(site_url, href)
                    
                    if urlparse(full_url).netloc == urlparse(site_url).netloc:
                        if self.is_contact_url(full_url, text):
                            contact_urls.add(full_url)
            
            # 結果を保存
            result = {
                'company_name': company_name,
                'official_site_url': site_url,
                'contact_urls': list(contact_urls),
                'contact_count': len(contact_urls),
                'status': 'success'
            }
            
            return result
            
        except requests.exceptions.Timeout:
            return {
                'company_name': company_name,
                'official_site_url': site_url,
                'contact_urls': [],
                'contact_count': 0,
                'status': 'timeout'
            }
        except Exception as e:
            return {
                'company_name': company_name,
                'official_site_url': site_url,
                'contact_urls': [],
                'contact_count': 0,
                'status': f'error: {str(e)[:100]}'
            }
    
    def process_companies(self, companies, limit=None):
        """複数の企業を並列処理"""
        
        if limit:
            companies = companies[:limit]
        
        total = len(companies)
        print(f"処理開始: {total}社")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self.extract_from_site,
                    company['company_name'],
                    company['official_site_url']
                ): company
                for company in companies
            }
            
            completed = 0
            for future in as_completed(futures):
                completed += 1
                result = future.result()
                
                with self.lock:
                    self.results.append(result)
                
                # 進捗表示
                if completed % 10 == 0:
                    success_count = sum(1 for r in self.results if r['status'] == 'success')
                    found_count = sum(1 for r in self.results if r['contact_count'] > 0)
                    print(f"  進捗: {completed}/{total} 完了 | 成功: {success_count} | 問い合わせ発見: {found_count}")
        
        return self.results

def main():
    """メイン処理"""
    
    print("="*80)
    print("問い合わせフォームURL抽出")
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # CSVファイルから企業データを読み込み
    csv_file = 'yuryoweb_final_complete_20250814_001100.csv'
    companies = []
    
    print(f"\n企業データを読み込み中: {csv_file}")
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['official_site_url']:
                    companies.append({
                        'company_name': row['company_name'],
                        'official_site_url': row['official_site_url']
                    })
        
        print(f"  ✅ {len(companies):,}社のデータを読み込みました")
        
    except Exception as e:
        print(f"  ❌ エラー: {e}")
        return
    
    # まず100社でテスト
    print(f"\n最初の100社でテスト実行...")
    
    extractor = ContactExtractor(timeout=10, max_workers=5)
    results = extractor.process_companies(companies, limit=100)
    
    # 結果の集計
    success_count = sum(1 for r in results if r['status'] == 'success')
    found_count = sum(1 for r in results if r['contact_count'] > 0)
    timeout_count = sum(1 for r in results if r['status'] == 'timeout')
    error_count = sum(1 for r in results if 'error' in r['status'])
    
    print(f"\n結果サマリー:")
    print(f"  処理数: {len(results)}社")
    print(f"  成功: {success_count}社")
    print(f"  問い合わせURL発見: {found_count}社")
    print(f"  タイムアウト: {timeout_count}社")
    print(f"  エラー: {error_count}社")
    
    # 成功例を表示
    print(f"\n問い合わせURL発見例（最初の5社）:")
    found_examples = [r for r in results if r['contact_count'] > 0][:5]
    
    for i, result in enumerate(found_examples, 1):
        print(f"\n{i}. {result['company_name']}")
        print(f"   サイト: {result['official_site_url']}")
        print(f"   問い合わせURL数: {result['contact_count']}")
        for url in result['contact_urls'][:3]:  # 最初の3つまで表示
            print(f"   - {url}")
        if len(result['contact_urls']) > 3:
            print(f"   ... 他{len(result['contact_urls'])-3}件")
    
    # 結果をJSONに保存
    output_file = f"contact_urls_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ テスト結果を保存しました: {output_file}")
    
    # CSVでも保存（使いやすい形式）
    csv_output = f"contact_urls_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(csv_output, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['company_name', 'official_site_url', 'contact_url', 'status'])
        
        for result in results:
            if result['contact_urls']:
                # 問い合わせURLが見つかった場合は各URLを別行で記録
                for contact_url in result['contact_urls']:
                    writer.writerow([
                        result['company_name'],
                        result['official_site_url'],
                        contact_url,
                        result['status']
                    ])
            else:
                # 見つからなかった場合も記録
                writer.writerow([
                    result['company_name'],
                    result['official_site_url'],
                    '',
                    result['status']
                ])
    
    print(f"✅ CSV形式でも保存しました: {csv_output}")
    
    print(f"\n完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

if __name__ == "__main__":
    main()