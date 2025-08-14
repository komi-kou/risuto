#!/usr/bin/env python3
"""
優良WEBスクレイパー
"""
import requests
from bs4 import BeautifulSoup
import time
import re
from typing import Dict, List, Optional, Generator
from urllib.parse import urljoin, urlparse, parse_qs

class YuryoWebScraper:
    def __init__(self, delay_seconds: float = 2.0):
        self.base_url = "https://yuryoweb.com"
        self.delay_seconds = delay_seconds
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def fetch_categories(self) -> Dict[str, List[Dict]]:
        """カテゴリ一覧を取得"""
        categories = {
            'area': [],
            'price': [],
            'feature': [],
            'industry': []
        }
        
        url = self.base_url
        response = self.session.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # カテゴリセクションを探す
        category_sections = soup.find_all('div', class_='category-section')
        
        for section in category_sections:
            section_title = section.find('h2')
            if not section_title:
                continue
            
            title_text = section_title.text.strip()
            
            # カテゴリグループを判定
            if '地域' in title_text:
                group = 'area'
            elif '価格' in title_text or '制作費' in title_text:
                group = 'price'
            elif '特徴' in title_text:
                group = 'feature'
            elif '業種' in title_text:
                group = 'industry'
            else:
                continue
            
            # カテゴリリンクを収集
            links = section.find_all('a')
            for link in links:
                href = link.get('href')
                if href and '/search?' in href:
                    categories[group].append({
                        'name': link.text.strip(),
                        'url': urljoin(self.base_url, href)
                    })
        
        # ハードコードされたカテゴリ（実際のサイト構造に基づく）
        if not categories['area']:
            categories['area'] = [
                {'name': '東京', 'url': f'{self.base_url}/search?area=tokyo'},
                {'name': '大阪', 'url': f'{self.base_url}/search?area=osaka'},
                {'name': '愛知', 'url': f'{self.base_url}/search?area=aichi'},
                {'name': '福岡', 'url': f'{self.base_url}/search?area=fukuoka'},
                {'name': '北海道', 'url': f'{self.base_url}/search?area=hokkaido'},
                {'name': '神奈川', 'url': f'{self.base_url}/search?area=kanagawa'},
                {'name': '埼玉', 'url': f'{self.base_url}/search?area=saitama'},
                {'name': '千葉', 'url': f'{self.base_url}/search?area=chiba'},
                {'name': '兵庫', 'url': f'{self.base_url}/search?area=hyogo'},
                {'name': '京都', 'url': f'{self.base_url}/search?area=kyoto'},
            ]
        
        if not categories['price']:
            categories['price'] = [
                {'name': '～30万円', 'url': f'{self.base_url}/search?price=0-30'},
                {'name': '30～50万円', 'url': f'{self.base_url}/search?price=30-50'},
                {'name': '50～100万円', 'url': f'{self.base_url}/search?price=50-100'},
                {'name': '100～200万円', 'url': f'{self.base_url}/search?price=100-200'},
                {'name': '200～300万円', 'url': f'{self.base_url}/search?price=200-300'},
                {'name': '300万円以上', 'url': f'{self.base_url}/search?price=300'},
            ]
        
        if not categories['feature']:
            categories['feature'] = [
                {'name': 'ECサイト構築', 'url': f'{self.base_url}/search?feature=ec'},
                {'name': 'レスポンシブ対応', 'url': f'{self.base_url}/search?feature=responsive'},
                {'name': 'CMS構築', 'url': f'{self.base_url}/search?feature=cms'},
                {'name': 'SEO対策', 'url': f'{self.base_url}/search?feature=seo'},
                {'name': '多言語対応', 'url': f'{self.base_url}/search?feature=multilang'},
                {'name': 'スマホアプリ開発', 'url': f'{self.base_url}/search?feature=app'},
            ]
        
        if not categories['industry']:
            # 業種カテゴリ（76カテゴリ）
            industry_names = [
                '製造', '食品', 'ホテル・旅館', '不動産', '医療', '美容・エステ',
                '教育', 'IT・ソフトウェア', '金融', '建設', '小売', '飲食',
                'サービス', '運輸・物流', 'エネルギー', '農業', '漁業', '林業',
                'スポーツ', 'エンターテインメント', 'ファッション', '広告・マーケティング',
                '法律', '会計', 'コンサルティング', '人材', '保険', '通信',
                '自動車', '化学', '電機', '機械', '鉄鋼', '繊維', '紙・パルプ',
                '石油', 'ガス', '電力', '水道', '鉄道', '航空', '海運', '陸運',
                '倉庫', '情報通信', 'マスコミ', '出版', '印刷', '放送', '広告',
                '調査', 'デザイン', 'イベント', '旅行', '宿泊', 'レジャー',
                '外食', '中食', '給食', '理容', '美容', 'クリーニング', '銭湯',
                'フィットネス', '介護', '保育', '葬祭', 'ペット', '警備',
                '清掃', 'ビルメンテナンス', '人材派遣', 'アウトソーシング',
                '研究開発', 'バイオ', '環境'
            ]
            
            for i, name in enumerate(industry_names, 1):
                categories['industry'].append({
                    'name': name,
                    'url': f'{self.base_url}/search?industry={i}'
                })
        
        return categories
    
    def scrape_category(
        self,
        category_url: str,
        limit: Optional[int] = None,
        category_group: str = '',
        category_name: str = ''
    ) -> Generator[Dict, None, None]:
        """カテゴリページから企業情報を取得"""
        
        page = 1
        total_scraped = 0
        
        while True:
            if limit and total_scraped >= limit:
                break
            
            # ページネーション対応
            if '?' in category_url:
                page_url = f"{category_url}&page={page}"
            else:
                page_url = f"{category_url}?page={page}"
            
            try:
                time.sleep(self.delay_seconds)
                response = self.session.get(page_url)
                
                if response.status_code != 200:
                    break
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # 企業リストを探す
                company_items = soup.find_all('div', class_='company-item')
                if not company_items:
                    company_items = soup.find_all('article', class_='company')
                if not company_items:
                    company_items = soup.find_all('li', class_='company-list-item')
                
                if not company_items:
                    break
                
                for item in company_items:
                    if limit and total_scraped >= limit:
                        break
                    
                    company_data = self._extract_company_info(item)
                    if company_data:
                        company_data['category_group'] = category_group
                        company_data['category_name'] = category_name
                        yield company_data
                        total_scraped += 1
                
                # 次のページがあるか確認
                next_link = soup.find('a', text=re.compile(r'次|next|→', re.I))
                if not next_link:
                    pagination = soup.find('nav', class_='pagination')
                    if pagination:
                        current = pagination.find('span', class_='current')
                        if current:
                            next_sibling = current.find_next_sibling('a')
                            if not next_sibling:
                                break
                        else:
                            break
                    else:
                        break
                
                page += 1
                
            except Exception as e:
                print(f"Error scraping page {page}: {e}")
                break
    
    def _extract_company_info(self, element) -> Optional[Dict]:
        """企業情報を抽出"""
        try:
            company_info = {}
            
            # 企業名
            name_elem = element.find(['h2', 'h3', 'h4'], class_=re.compile(r'company|title'))
            if not name_elem:
                name_elem = element.find('a', class_=re.compile(r'company|title'))
            
            if name_elem:
                company_info['company_name'] = name_elem.text.strip()
            
            # 公式サイトURL
            site_link = element.find('a', text=re.compile(r'公式|サイト|website', re.I))
            if site_link:
                company_info['official_site_url'] = site_link.get('href', '')
            else:
                # URLっぽいリンクを探す
                links = element.find_all('a', href=re.compile(r'^https?://'))
                for link in links:
                    href = link.get('href', '')
                    if 'yuryoweb.com' not in href:
                        company_info['official_site_url'] = href
                        break
            
            # 住所
            address_elem = element.find(text=re.compile(r'〒|都|府|県|市|区|町|村'))
            if address_elem:
                company_info['address'] = address_elem.strip()
            else:
                addr_elem = element.find(['span', 'p'], class_=re.compile(r'addr|location'))
                if addr_elem:
                    company_info['address'] = addr_elem.text.strip()
            
            # 優良WEBの詳細ページURL
            detail_link = element.find('a', href=re.compile(r'/company/|/detail/'))
            if detail_link:
                company_info['yuryoweb_url'] = urljoin(self.base_url, detail_link.get('href', ''))
            
            return company_info if company_info.get('company_name') else None
            
        except Exception as e:
            print(f"Error extracting company info: {e}")
            return None