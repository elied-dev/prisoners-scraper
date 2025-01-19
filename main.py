import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import logging
from typing import List, Dict
from os import path, makedirs

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%dT%H:%M:%S.%fZ', encoding='utf-8')

# Map Hebrew fields to English keys
field_mapping = {
    'שם מלא': 'full_name',
    'מספר אסיר, מספר ת"ז (סוג ת"ז)': 'prisoner_id',
    'מגדר, גיל, אזור מגורים': 'demographics',
    'תאריך לידה': 'birth_date',
    'שיוך ארגוני': 'organization',
    'אזרחות ישראלית': 'israeli_citizenship',
    'עצור או שפוט': 'status',
    'משך תקופת מאסר לשפוטים (בתצוגה של ימים-חודשים-שנים)': 'sentence_duration',
    'תאריך מעצר': 'arrest_date',
    'עבירות': 'offenses',
    'ערכאת השיפוט': 'court',
    'מספר תיק פל"א או פ.א': 'case_number',
    'מספר תיק בית משפט': 'court_file_number',
    'האם מיועד או מיועדת לגירוש?': 'deportation_status'
}

class DynamicPrisonerScraper:
    def __init__(self, base_url: str):
        self.base_url = base_url

    async def init_browser(self):
        """Initialize the browser instance."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()

    async def close_browser(self):
        """Close all browser instances."""
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()

    async def get_page_data(self, skip: int) -> List[Dict]:
        """Fetch and extract data from a single page."""
        url = f"{self.base_url}?skip={skip}"
        
        try:
            # Navigate to the page
            await self.page.goto(url, wait_until="networkidle")
            logging.info(f"Page loaded: {url}")
            
            # Wait for the content to load
            await self.page.wait_for_selector('.row.row-gov.ordered-fields', timeout=30000)
            
            # Extract all prisoner rows
            rows = await self.page.query_selector_all('.row.row-gov.ordered-fields')
            logging.info(f"Found {len(rows)} rows on the page")
            
            prisoners_data = []
            for row in rows:
                data = {}
                
                # Get all field divs in the row
                fields = await row.query_selector_all('.col-12')
                
                for field in fields:
                    # Get label and value
                    label_elem = await field.query_selector('label')
                    value_elem = await field.query_selector('.error-txt')
                    if label_elem and value_elem:
                        label = await label_elem.inner_text()
                        value = await value_elem.inner_text()
                        english_key = field_mapping.get(label.strip())
                        if english_key:
                            data[english_key] = value.strip()

                prisoners_data.append(data)
                logging.info(f"Extracted data for prisoner: {data.get('full_name')}")
            
            return prisoners_data
            
        except Exception as e:
            logging.error(f"Error processing page with skip={skip}: {e}")
            return []
    
    async def scrape_all_pages(self, max_pages: int = None) -> List[Dict]:
        """Scrape all pages of the prisoner database."""
        await self.init_browser()
        
        all_prisoners = []
        skip = 0
        page_num = 1
        
        try:
            while True:
                logging.info(f"Scraping page {page_num}")
                
                prisoners = await self.get_page_data(skip)
                if not prisoners:
                    break
                
                all_prisoners.extend(prisoners)
                logging.info(f"Found {len(prisoners)} prisoners on page {page_num}")
                
                if max_pages and page_num >= max_pages:
                    break
                
                skip += 20  # Assuming 10 results per page
                page_num += 1
                
                # Be nice to the server
                await asyncio.sleep(2)
                
        finally:
            await self.close_browser()
        
        return all_prisoners

    def save_to_csv(self, data: List[Dict], filename: str):
        """Save the scraped data to a CSV file."""
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logging.info(f"Data saved to {filename}")

class PrisonerDataTransformer:
    def __init__(self):
        # Define mapping dictionaries
        self.organization_mapping = {
            'חמאס': 'HAMAS',
            'פת"ח': 'FATAH',
            'ג\'יהאד אסלאמי': 'ISLAMIC JIHAD',
            'ללא': 'NONE',
            'חז"ד': 'DFLP',
            'חז"ע': 'PFLP',
            'דאע"ש': 'ISIS',
        }
        
        self.citizenship_mapping = {
            'לא': 'NO',
            'כן': 'YES'
        }
        
        self.status_mapping = {
            'שפוט': 'JUDGED',
            'עצור מנהלי': 'ADMINISTRATIVE DETENTION',
            'עצור': 'DETAINED',
        }
        
        self.court_mapping = {
            'בית משפט צבאי': 'MILITARY',
            'בית משפט אזרחי': 'CIVILIAN'
        }
        
        self.deportation_mapping = {
            'לא': 'NO DEPORTATION',
            'כן, גירוש לצמיתות': 'PERMANENT DEPORTATION',
            'כן, גירוש מותנה': 'CONDITIONNAL DEPORTATION'
        }

        # New mappings for gender and location
        self.gender_mapping = {
            'זכר': 'MALE',
            'נקבה': 'FEMALE'
        }

        self.residence_mapping = {
            'יהודה': 'JUDEA',
            'רצ"ע': 'GAZA STRIP',
            'שומרון': 'SAMARIA',
            'י-ם': 'JERUSALEM',
            'חו"ל': 'ABROAD',
            'קו ירוק': 'GREEN LINE',
        }

    def safe_map(self, value: str, mapping: dict) -> str:
        """Map a value using a dictionary, returning the original value if no mapping exists."""
        if pd.isna(value):
            return None
        return mapping.get(value, value)

    def split_demographics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Split the demographics column into gender, age, and residence columns."""
        df_processed = df.copy()
        
        # Split the demographics column into temporary series
        split_data = df_processed['demographics'].str.split(',', expand=True)
        
        # Assign the split values to new columns, strip whitespace
        df_processed['gender'] = split_data[0].str.strip()
        df_processed['age'] = split_data[1].str.strip()
        df_processed['residence'] = split_data[2].str.strip()
        
        # Apply mappings to gender and residence
        df_processed['gender'] = df_processed['gender'].apply(lambda x: self.safe_map(x, self.gender_mapping))
        df_processed['residence'] = df_processed['residence'].apply(lambda x: self.safe_map(x, self.residence_mapping))
        
        # Drop the original demographics column
        df_processed.drop('demographics', axis=1, inplace=True)
        
        # Convert age to numeric, handling any errors
        df_processed['age'] = pd.to_numeric(df_processed['age'], errors='coerce')
        
        return df_processed

    def format_date(self, date_str: str) -> str:
        """Format date string to YYYY-MM-DD."""
        try:
            if pd.isna(date_str):
                return None
            # Parse the date string and format it
            date_obj = pd.to_datetime(date_str)
            return date_obj.strftime('%Y-%m-%d')
        except Exception:
            return None

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations to the dataframe."""
        # First split demographics
        df_transformed = self.split_demographics(df)
        
        # Format dates
        df_transformed['birth_date'] = df_transformed['birth_date'].apply(self.format_date)
        df_transformed['arrest_date'] = df_transformed['arrest_date'].apply(self.format_date)
        
        # Apply mappings using safe_map
        df_transformed['organization'] = df_transformed['organization'].apply(lambda x: self.safe_map(x, self.organization_mapping))
        df_transformed['israeli_citizenship'] = df_transformed['israeli_citizenship'].apply(lambda x: self.safe_map(x, self.citizenship_mapping))
        df_transformed['status'] = df_transformed['status'].apply(lambda x: self.safe_map(x, self.status_mapping))
        df_transformed['court'] = df_transformed['court'].apply(lambda x: self.safe_map(x, self.court_mapping))
        df_transformed['deportation_status'] = df_transformed['deportation_status'].apply(lambda x: self.safe_map(x, self.deportation_mapping))
        
        return df_transformed

    def validate_data(self, df: pd.DataFrame) -> dict:
        """Validate the transformed data and return statistics."""
        stats = {
            'total_records': len(df),
            'null_values': df.isnull().sum().to_dict(),
            'value_distributions': {
                'gender': df['gender'].value_counts().to_dict(),
                'residence': df['residence'].value_counts().to_dict(),
                'organization': df['organization'].value_counts().to_dict(),
                'israeli_citizenship': df['israeli_citizenship'].value_counts().to_dict(),
                'status': df['status'].value_counts().to_dict(),
                'court': df['court'].value_counts().to_dict(),
                'deportation_status': df['deportation_status'].value_counts().to_dict()
            },
            'age_stats': df['age'].describe().to_dict()
        }
        return stats

async def scrap_data():
    # look for file first
    if not path.exists('output'):
        makedirs('output')
    
    if path.exists('output/prisoners_data.csv'):
        logging.info("File already exists, skipping scraping")
        return
    
    base_url = 'https://www.gov.il/he/Departments/DynamicCollectors/is-db'
    scraper = DynamicPrisonerScraper(base_url)
    
    # Scrape the data (limit to 5 pages for testing)
    MAX_PAGES = None # 2
    prisoners = await scraper.scrape_all_pages(max_pages=MAX_PAGES)
    
    # Save the results
    scraper.save_to_csv(prisoners, 'output/prisoners_data.csv')

def transform_data():
    try:
        # Read the original CSV file
        input_file = 'output/prisoners_data.csv'
        df = pd.read_csv(input_file, encoding='utf-8-sig')
        
        # Create transformer instance and transform data
        transformer = PrisonerDataTransformer()
        df_transformed = transformer.transform_data(df)
        
        # Validate the transformed data
        validation_stats = transformer.validate_data(df_transformed)
        
        # Save transformed data
        output_file = 'output/prisoners_data_transformed.csv'
        df_transformed.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        # Output statistics and validation results to a markdown file
        stats_file = 'output/validation_stats.md'
        with open(stats_file, 'w', encoding='utf-8') as f:
            f.write(f"# Data Transformation Statistics\n\n")
            f.write(f"## Total Records\n")
            f.write(f"Total records: {validation_stats['total_records']}\n\n")
            
            f.write(f"## Value Distributions\n")
            for field, distribution in validation_stats['value_distributions'].items():
              f.write(f"### {field.replace('_', ' ').title()} Distribution\n")
              for value, count in distribution.items():
                f.write(f"- {value}: {count}\n")
            f.write("\n")
            
            f.write(f"## Age Statistics\n")
            age_stats = validation_stats['age_stats']
            f.write(f"- Average age: {age_stats['mean']:.1f}\n")
            f.write(f"- Minimum age: {age_stats['min']}\n")
            f.write(f"- Maximum age: {age_stats['max']}\n\n")
        
        logging.info(f"Successfully transformed data and saved to {output_file}")
        logging.info(f"Validation statistics saved to {stats_file}")
        
    except FileNotFoundError:
        logging.error(f"Error: Could not find the input file {input_file}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

async def main():
    await scrap_data()
    transform_data()
    

if __name__ == "__main__":
    asyncio.run(main())
