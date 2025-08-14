#!/usr/bin/env python3
"""
指定された5社の問い合わせURL取得テスト
"""
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime

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

def is_contact_url(url, text=''):
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

def extract_contact_urls(site_url):
    """1つのサイトから問い合わせURLを詳細抽出"""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    })
    
    try:
        # サイトにアクセス
        print(f"\nアクセス中: {site_url}")
        response = session.get(site_url, timeout=10, allow_redirects=True)
        response.raise_for_status()
        
        # リダイレクト先を表示
        if response.url != site_url:
            print(f"  リダイレクト先: {response.url}")
        
        # HTMLをパース
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # ページタイトル
        title = soup.find('title')
        if title:
            print(f"  ページタイトル: {title.text.strip()}")
        
        # 問い合わせ関連のリンクを収集
        contact_links = {}
        
        # 全てのリンクを確認
        all_links = soup.find_all('a', href=True)
        print(f"  総リンク数: {len(all_links)}")
        
        for link in all_links:
            href = link.get('href', '')
            if not href or href.startswith('#'):
                continue
            
            text = link.get_text(strip=True)
            
            # 相対URLを絶対URLに変換
            full_url = urljoin(response.url, href)
            
            # 同じドメイン内のみ
            if urlparse(full_url).netloc == urlparse(response.url).netloc:
                if is_contact_url(full_url, text):
                    # URLをキーとして、付随テキストを保存
                    if full_url not in contact_links:
                        contact_links[full_url] = []
                    if text and text not in contact_links[full_url]:
                        contact_links[full_url].append(text)
        
        # ナビゲーション領域も特別にチェック
        nav_areas = soup.find_all(['nav', 'header', 'footer'])
        for nav in nav_areas:
            links = nav.find_all('a', href=True)
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                full_url = urljoin(response.url, href)
                
                if urlparse(full_url).netloc == urlparse(response.url).netloc:
                    if is_contact_url(full_url, text):
                        if full_url not in contact_links:
                            contact_links[full_url] = []
                        if text and text not in contact_links[full_url]:
                            contact_links[full_url].append(text)
        
        return {
            'status': 'success',
            'site_url': site_url,
            'final_url': response.url,
            'contact_links': contact_links
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'site_url': site_url,
            'error': str(e),
            'contact_links': {}
        }

def main():
    """メイン処理"""
    
    print("="*80)
    print("5社の問い合わせURL取得テスト")
    print(f"実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # テスト対象の5社
    test_companies = [
        {'name': '!null(ノーヌル)', 'url': 'https://www.nonull.jp/'},
        {'name': '&Fostory', 'url': 'http://andfostory.com/'},
        {'name': '*Copyself', 'url': 'https://copyself.jp/'},
        {'name': '.THINK DESIGN', 'url': 'https://www.tn9.jp/'},
        {'name': '01DESIGN', 'url': 'http://01design.biz/'}
    ]
    
    # 各社を処理
    results = []
    
    for company in test_companies:
        print(f"\n{'='*60}")
        print(f"会社名: {company['name']}")
        
        result = extract_contact_urls(company['url'])
        result['company_name'] = company['name']
        results.append(result)
        
        if result['status'] == 'success':
            contact_links = result['contact_links']
            
            if contact_links:
                print(f"\n  ✅ 問い合わせURL発見: {len(contact_links)}個")
                
                for i, (url, texts) in enumerate(contact_links.items(), 1):
                    print(f"\n  [{i}] {url}")
                    if texts:
                        print(f"      リンクテキスト: {', '.join(texts[:3])}")
            else:
                print("\n  ❌ 問い合わせURLが見つかりませんでした")
        else:
            print(f"\n  ❌ エラー: {result.get('error', 'Unknown error')}")
    
    # サマリー
    print(f"\n{'='*80}")
    print("サマリー:")
    print(f"{'='*80}")
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    found_count = sum(1 for r in results if r['contact_links'])
    
    print(f"処理成功: {success_count}/5社")
    print(f"問い合わせURL発見: {found_count}/5社")
    
    print(f"\n詳細結果:")
    for result in results:
        name = result['company_name']
        status = "✅" if result['contact_links'] else "❌"
        count = len(result['contact_links'])
        
        print(f"\n{status} {name}")
        print(f"   URL: {result.get('site_url', '')}")
        
        if result['contact_links']:
            print(f"   問い合わせURL数: {count}")
            # 主要な問い合わせURLを1つ表示
            main_url = list(result['contact_links'].keys())[0]
            print(f"   主要URL: {main_url}")
    
    print(f"\n完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

if __name__ == "__main__":
    main()