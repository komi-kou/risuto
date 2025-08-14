#!/usr/bin/env python3
"""
全企業の問い合わせURL抽出して最終CSVに追加
"""
import csv
import requests
from bs4 import BeautifulSoup
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

# 除外するキーワード
EXCLUDE_KEYWORDS = [
    'privacy', 'policy', 'terms', 'プライバシー', '規約', '利用規約',
    'sitemap', 'サイトマップ', 'recruit', '採用', 'career', 'login', 'ログイン'
]

class ContactExtractor:
    def __init__(self, timeout=7, max_workers=15):
        self.timeout = timeout
        self.max_workers = max_workers
        self.lock = threading.Lock()
        self.results = {}
        self.processed = 0
        self.success = 0
        self.found = 0
        self.start_time = time.time()
        
    def create_session(self):
        """新しいセッションを作成"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        return session
        
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
    
    def extract_from_site(self, company):
        """1つのサイトから問い合わせURLを抽出"""
        session = self.create_session()
        site_url = company['official_site_url']
        
        try:
            # URLの正規化
            if not site_url.startswith(('http://', 'https://')):
                site_url = 'https://' + site_url
            
            # サイトにアクセス
            response = session.get(site_url, timeout=self.timeout, allow_redirects=True)
            response.raise_for_status()
            
            # HTMLをパース
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 問い合わせ関連のリンクを収集
            contact_urls = set()
            
            # 全てのリンクを確認
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if not href or href.startswith('#'):
                    continue
                    
                text = link.get_text(strip=True)
                
                # 相対URLを絶対URLに変換
                full_url = urljoin(response.url, href)
                
                # 同じドメイン内のみ
                if urlparse(full_url).netloc == urlparse(response.url).netloc:
                    if self.is_contact_url(full_url, text):
                        contact_urls.add(full_url)
                        # 最初の1つだけで十分
                        if contact_urls:
                            break
            
            # トップページ自体が問い合わせフォームの場合をチェック
            if not contact_urls:
                page_text = soup.get_text().lower()
                if any(keyword in page_text for keyword in ['お名前', '会社名', 'メールアドレス', 'お問い合わせ内容', 'name', 'email', 'message']):
                    # フォーム要素があるか確認
                    if soup.find('form'):
                        contact_urls.add(response.url)
            
            # 結果を保存
            contact_url = list(contact_urls)[0] if contact_urls else ''
            
            with self.lock:
                self.results[company['official_site_url']] = contact_url
                self.processed += 1
                self.success += 1
                if contact_url:
                    self.found += 1
                
                # 進捗表示（100件ごと）
                if self.processed % 100 == 0:
                    elapsed = time.time() - self.start_time
                    rate = self.processed / elapsed
                    remaining = (10021 - self.processed) / rate
                    print(f"  進捗: {self.processed:,}/10,021件 | 成功: {self.success:,} | 発見: {self.found:,} | 残り時間: {remaining/60:.1f}分")
            
            return contact_url
            
        except:
            with self.lock:
                self.results[company['official_site_url']] = ''
                self.processed += 1
                
                if self.processed % 100 == 0:
                    elapsed = time.time() - self.start_time
                    rate = self.processed / elapsed
                    remaining = (10021 - self.processed) / rate
                    print(f"  進捗: {self.processed:,}/10,021件 | 成功: {self.success:,} | 発見: {self.found:,} | 残り時間: {remaining/60:.1f}分")
            
            return ''
        finally:
            session.close()
    
    def process_all_companies(self, companies):
        """全企業を並列処理"""
        
        total = len(companies)
        print(f"処理開始: {total:,}社")
        print(f"並列数: {self.max_workers}")
        print(f"推定時間: 20-30分")
        print("-" * 60)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.extract_from_site, company): company
                for company in companies
            }
            
            for future in as_completed(futures):
                future.result()
        
        return self.results

def main():
    """メイン処理"""
    
    print("="*80)
    print("問い合わせURL抽出 & 最終CSV作成")
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # 既存のCSVファイルから企業データを読み込み
    input_csv = 'yuryoweb_final_complete_20250814_001100.csv'
    companies = []
    company_data = {}
    
    print(f"\n企業データを読み込み中: {input_csv}")
    
    try:
        with open(input_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                companies.append({
                    'company_name': row['company_name'],
                    'official_site_url': row['official_site_url']
                })
                company_data[row['official_site_url']] = row
        
        print(f"  ✅ {len(companies):,}社のデータを読み込みました")
        
    except Exception as e:
        print(f"  ❌ エラー: {e}")
        return
    
    # 処理実行
    print(f"\n問い合わせURL抽出を開始します...")
    start_time = time.time()
    
    extractor = ContactExtractor(timeout=7, max_workers=15)
    contact_results = extractor.process_all_companies(companies)
    
    elapsed = time.time() - start_time
    
    # 結果の集計
    found_count = sum(1 for url in contact_results.values() if url)
    
    print(f"\n{'='*60}")
    print("抽出完了:")
    print(f"  処理企業数: {len(contact_results):,}社")
    print(f"  問い合わせURL発見: {found_count:,}社 ({found_count/len(contact_results)*100:.1f}%)")
    print(f"  処理時間: {elapsed:.1f}秒 ({elapsed/60:.1f}分)")
    print(f"{'='*60}")
    
    # 最終CSVを作成（問い合わせURL付き）
    output_file = f"yuryoweb_complete_with_contact_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    print(f"\n最終CSVを作成中...")
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        # ヘッダー（問い合わせURL列を追加）
        writer.writerow([
            'company_name',
            'official_site_url',
            'contact_url',  # 新規追加
            'yuryoweb_url',
            'address',
            'area_categories',
            'price_categories',
            'feature_categories',
            'industry_categories'
        ])
        
        # データを書き込み
        for url, company_info in company_data.items():
            contact_url = contact_results.get(url, '')
            
            writer.writerow([
                company_info['company_name'],
                company_info['official_site_url'],
                contact_url,  # 問い合わせURL
                company_info['yuryoweb_url'],
                company_info['address'],
                company_info['area_categories'],
                company_info['price_categories'],
                company_info['feature_categories'],
                company_info['industry_categories']
            ])
    
    print(f"✅ 最終CSV作成完了: {output_file}")
    
    # サマリーCSVも作成（シンプル版）
    summary_file = f"contact_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(summary_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['company_name', 'official_site_url', 'contact_url'])
        
        for url, contact_url in contact_results.items():
            company_name = company_data[url]['company_name']
            writer.writerow([company_name, url, contact_url])
    
    print(f"✅ サマリーCSV作成完了: {summary_file}")
    
    # 統計レポート
    stats_file = f"extraction_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write("問い合わせURL抽出 完了レポート\n")
        f.write("="*60 + "\n")
        f.write(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"処理企業数: {len(contact_results):,}社\n")
        f.write(f"問い合わせURL発見: {found_count:,}社 ({found_count/len(contact_results)*100:.1f}%)\n")
        f.write(f"問い合わせURL未発見: {len(contact_results)-found_count:,}社\n")
        f.write(f"処理時間: {elapsed:.1f}秒 ({elapsed/60:.1f}分)\n")
        f.write(f"平均処理速度: {len(contact_results)/elapsed:.1f}社/秒\n")
    
    print(f"✅ 統計レポート保存: {stats_file}")
    
    print(f"\n🎉 全処理完了！")
    print(f"完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

if __name__ == "__main__":
    main()