#!/usr/bin/env python3
"""
全企業の問い合わせフォームURL抽出（高速版）
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

# 除外するキーワード
EXCLUDE_KEYWORDS = [
    'privacy', 'policy', 'terms', 'プライバシー', '規約', '利用規約',
    'sitemap', 'サイトマップ', 'recruit', '採用', 'career', 'login', 'ログイン'
]

class ContactExtractor:
    def __init__(self, timeout=8, max_workers=10):
        self.timeout = timeout
        self.max_workers = max_workers
        self.lock = threading.Lock()
        self.results = []
        self.processed = 0
        self.success = 0
        self.found = 0
        
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
    
    def extract_from_site(self, company_name, site_url):
        """1つのサイトから問い合わせURLを抽出"""
        session = self.create_session()
        
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
            
            # 全てのリンクを確認（最適化版）
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
            
            # 結果を保存
            result = {
                'company_name': company_name,
                'official_site_url': site_url,
                'contact_urls': list(contact_urls)[:5],  # 最大5個まで
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
                'status': f'error'
            }
        finally:
            session.close()
    
    def process_batch(self, companies, batch_num):
        """バッチ処理"""
        batch_results = []
        
        for company in companies:
            result = self.extract_from_site(
                company['company_name'],
                company['official_site_url']
            )
            batch_results.append(result)
            
            with self.lock:
                self.processed += 1
                if result['status'] == 'success':
                    self.success += 1
                if result['contact_count'] > 0:
                    self.found += 1
                
                # 進捗表示（100件ごと）
                if self.processed % 100 == 0:
                    print(f"  進捗: {self.processed:,}件完了 | 成功: {self.success:,} | 発見: {self.found:,}")
        
        return batch_results
    
    def process_all_companies(self, companies):
        """全企業を並列処理"""
        
        total = len(companies)
        print(f"処理開始: {total:,}社")
        print(f"並列数: {self.max_workers}")
        
        # バッチに分割
        batch_size = max(1, total // (self.max_workers * 10))
        batches = [companies[i:i+batch_size] for i in range(0, total, batch_size)]
        
        all_results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.process_batch, batch, i): i
                for i, batch in enumerate(batches)
            }
            
            for future in as_completed(futures):
                batch_results = future.result()
                all_results.extend(batch_results)
        
        return all_results

def main():
    """メイン処理"""
    
    print("="*80)
    print("問い合わせフォームURL抽出（全企業版）")
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
    
    # 実行確認
    print(f"\n{len(companies):,}社の処理を開始します。")
    print("推定処理時間: 約20-30分")
    
    # 処理実行
    start_time = time.time()
    extractor = ContactExtractor(timeout=8, max_workers=10)
    results = extractor.process_all_companies(companies)
    elapsed = time.time() - start_time
    
    # 結果の集計
    success_count = sum(1 for r in results if r['status'] == 'success')
    found_count = sum(1 for r in results if r['contact_count'] > 0)
    timeout_count = sum(1 for r in results if r['status'] == 'timeout')
    error_count = sum(1 for r in results if r['status'] == 'error')
    
    print(f"\n{'='*60}")
    print("最終結果:")
    print(f"  処理企業数: {len(results):,}社")
    print(f"  成功: {success_count:,}社 ({success_count/len(results)*100:.1f}%)")
    print(f"  問い合わせURL発見: {found_count:,}社 ({found_count/len(results)*100:.1f}%)")
    print(f"  タイムアウト: {timeout_count:,}社")
    print(f"  エラー: {error_count:,}社")
    print(f"  処理時間: {elapsed:.1f}秒 ({elapsed/60:.1f}分)")
    print(f"{'='*60}")
    
    # 結果をJSONに保存（詳細版）
    output_file = f"contact_urls_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ 詳細結果を保存: {output_file}")
    
    # CSVで保存（実用版）
    csv_output = f"contact_urls_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(csv_output, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'company_name',
            'official_site_url',
            'main_contact_url',
            'other_contact_urls',
            'contact_count',
            'status'
        ])
        
        for result in results:
            main_contact = result['contact_urls'][0] if result['contact_urls'] else ''
            other_contacts = '｜'.join(result['contact_urls'][1:]) if len(result['contact_urls']) > 1 else ''
            
            writer.writerow([
                result['company_name'],
                result['official_site_url'],
                main_contact,
                other_contacts,
                result['contact_count'],
                result['status']
            ])
    
    print(f"✅ 最終CSV保存: {csv_output}")
    
    # 統計情報も保存
    stats_file = f"contact_extraction_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write("問い合わせURL抽出 統計レポート\n")
        f.write("="*60 + "\n")
        f.write(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"処理企業数: {len(results):,}社\n")
        f.write(f"成功: {success_count:,}社 ({success_count/len(results)*100:.1f}%)\n")
        f.write(f"問い合わせURL発見: {found_count:,}社 ({found_count/len(results)*100:.1f}%)\n")
        f.write(f"タイムアウト: {timeout_count:,}社\n")
        f.write(f"エラー: {error_count:,}社\n")
        f.write(f"処理時間: {elapsed:.1f}秒 ({elapsed/60:.1f}分)\n")
        f.write(f"平均処理速度: {len(results)/elapsed:.1f}社/秒\n")
    
    print(f"✅ 統計レポート保存: {stats_file}")
    
    print(f"\n完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

if __name__ == "__main__":
    main()