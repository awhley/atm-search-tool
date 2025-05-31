import streamlit as st
import pandas as pd
import requests
import math
import numpy as np
from io import BytesIO
import time

# Configure the Streamlit page
st.set_page_config(
    page_title="ATM Location Search",
    page_icon="🏧",
    layout="wide"
)

class ATMSearchTool:
    def __init__(self):
        self.df = None
        self.invalid_zips_df = None
        self.zip_coords_cache = {}
        
    def get_zip_coordinates(self, zip_code):
        """Get coordinates for a zip code using free API"""
        if zip_code in self.zip_coords_cache:
            return self.zip_coords_cache[zip_code]
        
        try:
            # Use free zip code API
            url = f"https://api.zippopotam.us/us/{zip_code}"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                lat = float(data['places'][0]['latitude'])
                lon = float(data['places'][0]['longitude'])
                coords = {'latitude': lat, 'longitude': lon}
                self.zip_coords_cache[zip_code] = coords
                return coords
            else:
                self.zip_coords_cache[zip_code] = {'latitude': None, 'longitude': None}
                return {'latitude': None, 'longitude': None}
                
        except Exception as e:
            self.zip_coords_cache[zip_code] = {'latitude': None, 'longitude': None}
            return {'latitude': None, 'longitude': None}
    
    def haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points using Haversine formula"""
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Radius of earth in miles
        r = 3956
        
        return c * r
        
    def load_excel_file(self, uploaded_file):
        """Load and process the Excel file"""
        try:
            # Read Excel file
            self.df = pd.read_excel(uploaded_file)
            
            # Clean and standardize column names (case insensitive)
            self.df.columns = self.df.columns.str.strip().str.lower()
            
            # Map common column variations to expected names
            column_mapping = {
                'terminal': 'terminal',
                'customer code': 'customer_code',
                'ownership': 'ownership',
                'location': 'location',
                'address': 'address',
                'city': 'city',
                'st': 'state',
                'zip': 'zip',
                'zip long': 'zip_long',
                'zip short': 'zip_short',
                'dma code': 'dma_code',
                'dma description': 'dma_description',
                'make': 'make',
                'model': 'model',
                'parent chain business': 'parent_chain_business',
                'naics category': 'naics_category',
                'naics sector': 'naics_sector',
                'lob': 'lob',
                'cbm category': 'cbm_category',
                'cbm level': 'cbm_level',
                'avg transactions': 'avg_transactions',
                'avg cash dispensed': 'avg_cash_dispensed',
                'most recent month trx': 'most_recent_month_trx',
                'most recent month cd': 'most_recent_month_cd',
                'machine style code (3 digit code)': 'machine_style_code',
                'display surfaces code (lcr)': 'display_surfaces_code',
                'location type code (2 digits)': 'location_type_code',
                'permanent or tenmp (perm 1, temp 0)': 'permanent_or_temp',
                'inside or outside ( 1 inside, 0 outside)': 'inside_or_outside'
            }
            
            # Rename columns
            self.df = self.df.rename(columns=column_mapping)
            
            # Check for required columns
            required_cols = ['terminal', 'location', 'address', 'city', 'state']
            missing_cols = [col for col in required_cols if col not in self.df.columns]
            
            if missing_cols:
                st.error(f"Missing required columns: {missing_cols}")
                return False
            
            # Handle zip codes - prefer zip_short, fallback to zip
            if 'zip_short' in self.df.columns:
                self.df['working_zip'] = self.df['zip_short']
                zip_source = 'zip_short'
            elif 'zip' in self.df.columns:
                self.df['working_zip'] = self.df['zip']
                zip_source = 'zip'
            else:
                st.error("No zip code column found (looking for 'zip_short' or 'zip')")
                return False
            
            # Process and validate zip codes
            self.process_zip_codes(zip_source)
            
            # Add lat/lon columns if they don't exist
            if 'latitude' not in self.df.columns or 'longitude' not in self.df.columns:
                self.add_coordinates()
                
            return True
            
        except Exception as e:
            st.error(f"Error loading file: {str(e)}")
            return False
    
    def process_zip_codes(self, zip_source):
        """Process and validate zip codes, separating valid from invalid"""
        
        st.info(f"Processing zip codes from '{zip_source}' column...")
        
        # Convert to string and handle various formats
        self.df['working_zip'] = self.df['working_zip'].astype(str)
        
        # Clean zip codes - remove any non-digit characters except hyphens
        self.df['working_zip'] = self.df['working_zip'].str.replace(r'[^\d-]', '', regex=True)
        
        # Handle different zip code formats
        def clean_zip(zip_code):
            if pd.isna(zip_code) or str(zip_code).lower() in ['nan', 'none', '']:
                return None
            
            zip_str = str(zip_code).strip()
            
            # Remove any leading/trailing spaces
            zip_str = zip_str.strip()
            
            # Handle ZIP+4 format (12345-6789 -> 12345)
            if '-' in zip_str:
                zip_str = zip_str.split('-')[0]
            
            # Pad with leading zeros if less than 5 digits
            if zip_str.isdigit():
                if len(zip_str) <= 5:
                    return zip_str.zfill(5)
                elif len(zip_str) == 9:  # Handle 9-digit zip without hyphen
                    return zip_str[:5]
            
            return None
        
        # Apply cleaning function
        self.df['cleaned_zip'] = self.df['working_zip'].apply(clean_zip)
        
        # Identify valid and invalid zip codes
        valid_mask = (
            self.df['cleaned_zip'].notna() & 
            (self.df['cleaned_zip'].str.len() == 5) & 
            (self.df['cleaned_zip'].str.isdigit())
        )
        
        # Separate valid and invalid records
        valid_df = self.df[valid_mask].copy()
        invalid_df = self.df[~valid_mask].copy()
        
        # Store invalid zip codes for review
        if len(invalid_df) > 0:
            self.invalid_zips_df = invalid_df.copy()
            
            # Create a summary of invalid zip issues
            invalid_df['zip_issue'] = invalid_df.apply(lambda row: self.diagnose_zip_issue(row['working_zip'], row['cleaned_zip']), axis=1)
            
            st.warning(f"⚠️ Found {len(invalid_df)} records with invalid zip codes")
            
            # Show summary of issues
            issue_summary = invalid_df['zip_issue'].value_counts()
            st.write("**Invalid zip code issues:**")
            for issue, count in issue_summary.items():
                st.write(f"- {issue}: {count} records")
        else:
            st.success("✅ All zip codes are valid")
        
        # Update main dataframe to only include valid records
        self.df = valid_df.copy()
        self.df['zip'] = self.df['cleaned_zip']  # Use cleaned zip as the main zip column
        
        st.info(f"Processed {len(self.df)} records with valid zip codes")
    
    def diagnose_zip_issue(self, original_zip, cleaned_zip):
        """Diagnose what's wrong with an invalid zip code"""
        if pd.isna(original_zip) or str(original_zip).lower() in ['nan', 'none', '']:
            return "Missing/Empty zip code"
        
        original_str = str(original_zip).strip()
        
        if not original_str:
            return "Empty zip code"
        
        if not any(c.isdigit() for c in original_str):
            return "No digits in zip code"
        
        digits_only = ''.join(c for c in original_str if c.isdigit())
        
        if len(digits_only) < 5:
            return f"Too few digits ({len(digits_only)} digits)"
        
        if len(digits_only) > 9:
            return f"Too many digits ({len(digits_only)} digits)"
        
        if len(digits_only) in [6, 7, 8]:
            return f"Unusual zip length ({len(digits_only)} digits)"
        
        return "Other zip format issue"
    
    def add_coordinates(self):
        """Add latitude and longitude coordinates for all ATMs"""
        st.info("Adding coordinates for ATM locations... This may take a moment.")
        
        # Get unique zip codes to minimize API calls
        unique_zips = self.df['zip'].unique()
        
        # Create a mapping of zip codes to coordinates
        zip_coords = {}
        progress_bar = st.progress(0)
        
        failed_zips = []
        
        for i, zip_code in enumerate(unique_zips):
            # Add small delay to be respectful to the API
            if i > 0 and i % 10 == 0:
                time.sleep(1)
            
            coords = self.get_zip_coordinates(zip_code)
            zip_coords[zip_code] = coords
            
            if coords['latitude'] is None:
                failed_zips.append(zip_code)
            
            progress_bar.progress((i + 1) / len(unique_zips))
        
        # Map coordinates to the dataframe
        self.df['latitude'] = self.df['zip'].map(lambda x: zip_coords.get(x, {}).get('latitude'))
        self.df['longitude'] = self.df['zip'].map(lambda x: zip_coords.get(x, {}).get('longitude'))
        
        if failed_zips:
            st.warning(f"Could not find coordinates for {len(failed_zips)} zip codes: {failed_zips[:10]}{'...' if len(failed_zips) > 10 else ''}")
        
        st.success("Coordinates added successfully!")
    
    def search_atms_by_radius(self, search_zip, radius_miles):
        """Search for ATMs within specified radius of a zip code"""
        try:
            # Get coordinates for search zip code
            search_coords = self.get_zip_coordinates(search_zip)
            
            if search_coords['latitude'] is None:
                st.error(f"Could not find coordinates for zip code: {search_zip}")
                return pd.DataFrame()
            
            search_lat = search_coords['latitude']
            search_lon = search_coords['longitude']
            
            # Calculate distances for all ATMs
            distances = []
            
            for _, row in self.df.iterrows():
                if pd.notna(row['latitude']) and pd.notna(row['longitude']):
                    # Calculate distance using Haversine formula
                    distance_miles = self.haversine_distance(
                        search_lat, search_lon, 
                        row['latitude'], row['longitude']
                    )
                    distances.append(distance_miles)
                else:
                    distances.append(float('inf'))
            
            # Add distance column
            self.df['distance_miles'] = distances
            
            # Filter by radius and remove invalid distances
            filtered_df = self.df[
                (self.df['distance_miles'] <= radius_miles) & 
                (self.df['distance_miles'] != float('inf'))
            ].copy()
            
            # Sort by distance
            filtered_df = filtered_df.sort_values('distance_miles')
            
            # Round distance for display
            filtered_df['distance_miles'] = filtered_df['distance_miles'].round(2)
            
            return filtered_df
            
        except Exception as e:
            st.error(f"Error searching ATMs: {str(e)}")
            return pd.DataFrame()
    
    def export_results(self, results_df):
        """Export search results to Excel"""
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            results_df.to_excel(writer, sheet_name='ATM_Search_Results', index=False)
            
        return output.getvalue()
    
    def export_invalid_zips(self):
        """Export invalid zip codes to Excel for review"""
        if self.invalid_zips_df is not None:
            output = BytesIO()
            
            # Add issue diagnosis
            export_df = self.invalid_zips_df.copy()
            export_df['zip_issue'] = export_df.apply(lambda row: self.diagnose_zip_issue(row['working_zip'], row.get('cleaned_zip')), axis=1)
            
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                export_df.to_excel(writer, sheet_name='Invalid_Zip_Codes', index=False)
                
            return output.getvalue()
        return None

def main():
    st.title("🏧 ATM Location Search Tool")
    st.markdown("Search for ATMs within a specified radius of any US zip code")
    
    # Initialize the search tool
    if 'search_tool' not in st.session_state:
        st.session_state.search_tool = ATMSearchTool()
    
    search_tool = st.session_state.search_tool
    
    # Sidebar for file upload and configuration
    with st.sidebar:
        st.header("Configuration")
        
        # File upload
        st.subheader("Upload ATM Data")
        uploaded_file = st.file_uploader(
            "Choose Excel file",
            type=['xlsx', 'xls'],
            help="Upload your Excel file containing ATM data"
        )
        
        if uploaded_file is not None:
            if st.button("Load Data"):
                with st.spinner("Loading ATM data..."):
                    if search_tool.load_excel_file(uploaded_file):
                        st.success(f"✅ Loaded {len(search_tool.df)} ATM records with valid zip codes")
                        st.session_state.data_loaded = True
                        
                        # Show invalid zips download option
                        if search_tool.invalid_zips_df is not None:
                            invalid_count = len(search_tool.invalid_zips_df)
                            st.warning(f"⚠️ {invalid_count} records had invalid zip codes")
                            
                            invalid_zip_data = search_tool.export_invalid_zips()
                            if invalid_zip_data:
                                st.download_button(
                                    label=f"📥 Download {invalid_count} Invalid Zip Records",
                                    data=invalid_zip_data,
                                    file_name="invalid_zip_codes.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    help="Download records with invalid zip codes for review"
                                )
                    else:
                        st.session_state.data_loaded = False
        
        # Data info section
        if hasattr(st.session_state, 'data_loaded') and st.session_state.data_loaded:
            st.subheader("Data Information")
            
            # Show zip code source
            if 'zip_short' in search_tool.df.columns:
                st.info("Using 'Zip Short' column for searches")
            else:
                st.info("Using 'Zip' column for searches")
            
            # Show coordinate status
            valid_coords = search_tool.df.dropna(subset=['latitude', 'longitude'])
            coord_percentage = (len(valid_coords) / len(search_tool.df)) * 100
            st.metric("Coordinate Coverage", f"{coord_percentage:.1f}%")
    
    # Main search interface
    if hasattr(st.session_state, 'data_loaded') and st.session_state.data_loaded:
        
        # Display data summary
        st.subheader("📊 Data Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total ATMs", len(search_tool.df))
        with col2:
            st.metric("Unique Zip Codes", search_tool.df['zip'].nunique())
        with col3:
            st.metric("States Covered", search_tool.df['state'].nunique())
        with col4:
            if search_tool.invalid_zips_df is not None:
                st.metric("Invalid Zip Records", len(search_tool.invalid_zips_df))
            else:
                st.metric("Invalid Zip Records", 0)
        
        # Search interface
        st.subheader("🔍 Search ATMs")
        
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            search_zip = st.text_input(
                "Enter Zip Code",
                placeholder="e.g., 10001",
                help="Enter a 5-digit US zip code"
            )
        
        with col2:
            radius_miles = st.selectbox(
                "Select Radius",
                [5, 10, 15, 20, 25, 30, 50],
                index=1,
                help="Select search radius in miles"
            )
        
        with col3:
            st.write("")  # Spacing
            search_button = st.button("🔍 Search", type="primary")
        
        # Perform search
        if search_button and search_zip:
            # Validate zip code format
            if len(search_zip) == 5 and search_zip.isdigit():
                with st.spinner(f"Searching for ATMs within {radius_miles} miles of {search_zip}..."):
                    results = search_tool.search_atms_by_radius(search_zip, radius_miles)
                    
                    if not results.empty:
                        st.success(f"Found {len(results)} ATMs within {radius_miles} miles of {search_zip}")
                        
                        # Display results
                        st.subheader("Search Results")
                        
                        # Select key columns to display
                        key_columns = [
                            'terminal', 'location', 'address', 'city', 'state', 'zip', 
                            'distance_miles', 'make', 'model', 'avg_transactions', 
                            'avg_cash_dispensed', 'permanent_or_temp', 'inside_or_outside'
                        ]
                        
                        # Only show columns that exist in the data
                        display_columns = [col for col in key_columns if col in results.columns]
                        
                        # Display the filtered dataframe
                        st.dataframe(
                            results[display_columns],
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Download button
                        excel_data = search_tool.export_results(results)
                        st.download_button(
                            label="📥 Download Results as Excel",
                            data=excel_data,
                            file_name=f"atm_search_{search_zip}_{radius_miles}miles.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        
                        # Store results in session state for map display
                        st.session_state.search_results = results
                        
                    else:
                        st.warning(f"No ATMs found within {radius_miles} miles of {search_zip}")
            else:
                st.error("Please enter a valid 5-digit zip code")
        
        # Optional: Display map if results exist
        if hasattr(st.session_state, 'search_results') and not st.session_state.search_results.empty:
            try:
                st.subheader("📍 Map View")
                
                # Prepare map data
                map_data = st.session_state.search_results.dropna(subset=['latitude', 'longitude'])
                
                if not map_data.empty:
                    st.map(
                        map_data[['latitude', 'longitude']],
                        use_container_width=True
                    )
                else:
                    st.info("Map not available - coordinate data missing")
                    
            except Exception as e:
                st.info("Map display not available")
        
    else:
        # Welcome screen
        st.info("👆 Please upload your ATM Excel file using the sidebar to get started")
        
        st.subheader("Expected Excel File Format")
        st.markdown("""
        Your Excel file should contain the following columns:
        - **Terminal**: Terminal ID
        - **Location**: Location name/description
        - **Address**: Street address
        - **City**: City name
        - **St**: State abbreviation
        - **Zip Short**: 5-digit zip code (preferred) or **Zip**: zip code
        - And other optional columns like Make, Model, etc.
        """)

if __name__ == "__main__":
    main()