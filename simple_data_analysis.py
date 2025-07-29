#!/usr/bin/env python3
"""
Simple Coral Gables Property Data Analysis
Uses only built-in Python libraries to analyze property data
"""

import json
import os
from collections import Counter, defaultdict
from typing import Dict, List, Any
import statistics

class SimpleCoralGablesAnalyzer:
    def __init__(self, input_file: str = "CGProperties.json"):
        self.input_file = input_file
        self.data = {}
        self.features = []
        self.property_data = []
        
    def load_data(self):
        """Load the CGProperties GeoJSON file"""
        print(f"📁 Loading data from {self.input_file}...")
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            if self.data.get('type') == 'FeatureCollection':
                self.features = self.data.get('features', [])
                print(f"✅ Loaded {len(self.features)} features from GeoJSON FeatureCollection")
            else:
                print("⚠️ Warning: File is not a GeoJSON FeatureCollection")
                self.features = []
                
        except FileNotFoundError:
            print(f"❌ Error: File {self.input_file} not found")
            return False
        except json.JSONDecodeError as e:
            print(f"❌ Error: Invalid JSON format - {e}")
            return False
        return True
    
    def extract_property_data(self):
        """Extract property data into a list of dictionaries"""
        print("🔄 Extracting property data...")
        
        for feature in self.features:
            properties = feature.get('properties', {})
            
            # Extract key property information
            prop_info = {
                'address': properties.get('AddressPoints.ADDRESS', 'Unknown'),
                'owner': properties.get('AddressPoints.TRUE_OWNER', 'Unknown'),
                'zoning': properties.get('AddressPoints.PRIMARY_ZO', 'Unknown'),
                'municipality': properties.get('AddressPoints.MUNICIPALI', 'Unknown'),
                'neighborhood': properties.get('AddressPoints.NEIGHBORHO', 'Unknown'),
                'year_built': properties.get('AddressPoints.YEAR_BUILT', 'Unknown'),
                'trash_route': properties.get('AddressPoints.TRASH_ROUT', 'Unknown'),
                'building_value': self.safe_float(properties.get('AddressPoints.BUILDING_V', 0)),
                'land_value': self.safe_float(properties.get('AddressPoints.LAND_VAL', 0)),
                'total_value': self.safe_float(properties.get('AddressPoints.TOTAL_VAL', 0)),
                'city_taxable': self.safe_float(properties.get('AddressPoints.CITY_TAXAB', 0)),
                'county_taxable': self.safe_float(properties.get('AddressPoints.CNTY_TAXAB', 0)),
                'flood_zone': properties.get('AddressPoints.FLOOD_ZONE', 'Unknown'),
                'land_use': properties.get('AddressPoints.LAND_USE', 'Unknown'),
                'square_footage': self.safe_float(properties.get('AddressPoints.SQ_FT', 0)),
                'bedrooms': self.safe_float(properties.get('AddressPoints.BEDROOMS', 0)),
                'bathrooms': self.safe_float(properties.get('AddressPoints.BATHROOMS', 0)),
                'stories': self.safe_float(properties.get('AddressPoints.STORIES', 0))
            }
            
            self.property_data.append(prop_info)
        
        print(f"✅ Extracted data for {len(self.property_data)} properties")
        return True
    
    def safe_float(self, value):
        """Safely convert value to float"""
        try:
            return float(value) if value and value != 'Unknown' else 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def basic_statistics(self):
        """Generate basic statistics about the property data"""
        print("\n📊 Generating basic statistics...")
        
        total_properties = len(self.property_data)
        properties_with_addresses = sum(1 for p in self.property_data if p['address'] != 'Unknown')
        properties_with_owners = sum(1 for p in self.property_data if p['owner'] != 'Unknown')
        properties_with_values = sum(1 for p in self.property_data if p['total_value'] > 0)
        properties_with_year_built = sum(1 for p in self.property_data if p['year_built'] != 'Unknown')
        
        # Count unique values
        unique_owners = len(set(p['owner'] for p in self.property_data))
        unique_zoning_types = len(set(p['zoning'] for p in self.property_data))
        unique_neighborhoods = len(set(p['neighborhood'] for p in self.property_data))
        unique_municipalities = len(set(p['municipality'] for p in self.property_data))
        
        # Value statistics
        values = [p['total_value'] for p in self.property_data if p['total_value'] > 0]
        if values:
            mean_value = statistics.mean(values)
            median_value = statistics.median(values)
            min_value = min(values)
            max_value = max(values)
            std_value = statistics.stdev(values) if len(values) > 1 else 0
        else:
            mean_value = median_value = min_value = max_value = std_value = 0
        
        # Year built statistics
        years = [p['year_built'] for p in self.property_data if p['year_built'] != 'Unknown']
        valid_years = []
        for year in years:
            try:
                year_int = int(year)
                if 1800 <= year_int <= 2024:  # Reasonable year range
                    valid_years.append(year_int)
            except (ValueError, TypeError):
                continue
        
        if valid_years:
            mean_year = statistics.mean(valid_years)
            median_year = statistics.median(valid_years)
            min_year = min(valid_years)
            max_year = max(valid_years)
        else:
            mean_year = median_year = min_year = max_year = 0
        
        stats = {
            'total_properties': total_properties,
            'properties_with_addresses': properties_with_addresses,
            'properties_with_owners': properties_with_owners,
            'properties_with_values': properties_with_values,
            'properties_with_year_built': properties_with_year_built,
            'unique_owners': unique_owners,
            'unique_zoning_types': unique_zoning_types,
            'unique_neighborhoods': unique_neighborhoods,
            'unique_municipalities': unique_municipalities,
            'value_statistics': {
                'mean_value': mean_value,
                'median_value': median_value,
                'min_value': min_value,
                'max_value': max_value,
                'std_value': std_value
            },
            'year_built_statistics': {
                'mean_year': mean_year,
                'median_year': median_year,
                'min_year': min_year,
                'max_year': max_year
            }
        }
        
        return stats
    
    def ownership_analysis(self):
        """Analyze property ownership patterns"""
        print("👥 Analyzing ownership patterns...")
        
        # Count properties per owner
        owner_counts = Counter(p['owner'] for p in self.property_data)
        
        # Top property owners
        top_owners = dict(owner_counts.most_common(20))
        
        # Owner types (individual vs corporate)
        def classify_owner(owner):
            if not owner or owner == 'Unknown':
                return 'Unknown'
            owner_str = str(owner).upper()
            corporate_indicators = ['LLC', 'INC', 'CORP', 'LTD', 'TRUST', 'ASSOCIATION', 'COMPANY', 'CO']
            if any(indicator in owner_str for indicator in corporate_indicators):
                return 'Corporate'
            else:
                return 'Individual'
        
        owner_types = Counter(classify_owner(p['owner']) for p in self.property_data)
        
        # Properties per owner statistics
        properties_per_owner = list(owner_counts.values())
        if properties_per_owner:
            mean_properties_per_owner = statistics.mean(properties_per_owner)
            median_properties_per_owner = statistics.median(properties_per_owner)
            max_properties_per_owner = max(properties_per_owner)
            owners_with_multiple_properties = sum(1 for count in properties_per_owner if count > 1)
        else:
            mean_properties_per_owner = median_properties_per_owner = max_properties_per_owner = owners_with_multiple_properties = 0
        
        ownership_stats = {
            'top_owners': top_owners,
            'owner_type_distribution': dict(owner_types),
            'properties_per_owner_stats': {
                'mean_properties_per_owner': mean_properties_per_owner,
                'median_properties_per_owner': median_properties_per_owner,
                'max_properties_per_owner': max_properties_per_owner,
                'owners_with_multiple_properties': owners_with_multiple_properties
            }
        }
        
        return ownership_stats
    
    def zoning_analysis(self):
        """Analyze zoning patterns"""
        print("🏗️ Analyzing zoning patterns...")
        
        # Zoning distribution
        zoning_counts = Counter(p['zoning'] for p in self.property_data)
        
        # Average property values by zoning
        zoning_values = defaultdict(list)
        for prop in self.property_data:
            if prop['total_value'] > 0:
                zoning_values[prop['zoning']].append(prop['total_value'])
        
        zoning_value_analysis = {}
        for zoning, values in zoning_values.items():
            if values:
                zoning_value_analysis[zoning] = {
                    'mean': statistics.mean(values),
                    'median': statistics.median(values),
                    'count': len(values)
                }
        
        # Most valuable zoning types
        top_zoning_by_value = sorted(
            [(zoning, data['mean']) for zoning, data in zoning_value_analysis.items()],
            key=lambda x: x[1], reverse=True
        )[:10]
        
        zoning_stats = {
            'zoning_distribution': dict(zoning_counts),
            'zoning_value_analysis': zoning_value_analysis,
            'top_zoning_by_value': dict(top_zoning_by_value)
        }
        
        return zoning_stats
    
    def neighborhood_analysis(self):
        """Analyze neighborhood patterns"""
        print("🏘️ Analyzing neighborhood patterns...")
        
        # Neighborhood distribution
        neighborhood_counts = Counter(p['neighborhood'] for p in self.property_data)
        
        # Average property values by neighborhood
        neighborhood_values = defaultdict(list)
        for prop in self.property_data:
            if prop['total_value'] > 0:
                neighborhood_values[prop['neighborhood']].append(prop['total_value'])
        
        neighborhood_value_analysis = {}
        for neighborhood, values in neighborhood_values.items():
            if values:
                neighborhood_value_analysis[neighborhood] = {
                    'mean': statistics.mean(values),
                    'median': statistics.median(values),
                    'count': len(values)
                }
        
        # Most valuable neighborhoods
        top_neighborhoods_by_value = sorted(
            [(neighborhood, data['mean']) for neighborhood, data in neighborhood_value_analysis.items()],
            key=lambda x: x[1], reverse=True
        )[:10]
        
        neighborhood_stats = {
            'neighborhood_distribution': dict(neighborhood_counts),
            'neighborhood_value_analysis': neighborhood_value_analysis,
            'top_neighborhoods_by_value': dict(top_neighborhoods_by_value)
        }
        
        return neighborhood_stats
    
    def value_analysis(self):
        """Analyze property values"""
        print("💰 Analyzing property values...")
        
        # Value ranges
        value_ranges = [
            (0, 100000, 'Under $100k'),
            (100000, 250000, '$100k-$250k'),
            (250000, 500000, '$250k-$500k'),
            (500000, 1000000, '$500k-$1M'),
            (1000000, 2500000, '$1M-$2.5M'),
            (2500000, 5000000, '$2.5M-$5M'),
            (5000000, float('inf'), 'Over $5M')
        ]
        
        value_distribution = {}
        for min_val, max_val, label in value_ranges:
            if max_val == float('inf'):
                count = sum(1 for p in self.property_data if p['total_value'] >= min_val and p['total_value'] > 0)
            else:
                count = sum(1 for p in self.property_data if min_val <= p['total_value'] < max_val)
            value_distribution[label] = count
        
        value_stats = {
            'value_distribution': value_distribution
        }
        
        return value_stats
    
    def generate_report(self, stats, ownership, zoning, neighborhood, value):
        """Generate a comprehensive analysis report"""
        print("\n📋 Generating comprehensive analysis report...")
        
        report = []
        report.append("=" * 80)
        report.append("CORAL GABLES PROPERTY DATA ANALYSIS REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Basic Statistics
        report.append("📊 BASIC STATISTICS")
        report.append("-" * 40)
        report.append(f"Total Properties: {stats['total_properties']:,}")
        report.append(f"Properties with Addresses: {stats['properties_with_addresses']:,}")
        report.append(f"Properties with Owners: {stats['properties_with_owners']:,}")
        report.append(f"Properties with Values: {stats['properties_with_values']:,}")
        report.append(f"Unique Owners: {stats['unique_owners']:,}")
        report.append(f"Unique Zoning Types: {stats['unique_zoning_types']:,}")
        report.append(f"Unique Neighborhoods: {stats['unique_neighborhoods']:,}")
        report.append("")
        
        if 'value_statistics' in stats:
            value_stats = stats['value_statistics']
            report.append("💰 VALUE STATISTICS")
            report.append("-" * 40)
            report.append(f"Mean Property Value: ${value_stats['mean_value']:,.2f}")
            report.append(f"Median Property Value: ${value_stats['median_value']:,.2f}")
            report.append(f"Minimum Property Value: ${value_stats['min_value']:,.2f}")
            report.append(f"Maximum Property Value: ${value_stats['max_value']:,.2f}")
            report.append(f"Standard Deviation: ${value_stats['std_value']:,.2f}")
            report.append("")
        
        # Ownership Analysis
        report.append("👥 OWNERSHIP ANALYSIS")
        report.append("-" * 40)
        report.append(f"Owners with Multiple Properties: {ownership['properties_per_owner_stats']['owners_with_multiple_properties']:,}")
        report.append(f"Mean Properties per Owner: {ownership['properties_per_owner_stats']['mean_properties_per_owner']:.2f}")
        report.append(f"Max Properties per Owner: {ownership['properties_per_owner_stats']['max_properties_per_owner']:,}")
        report.append("")
        
        report.append("Top 10 Property Owners:")
        for i, (owner, count) in enumerate(list(ownership['top_owners'].items())[:10], 1):
            report.append(f"  {i:2d}. {owner[:50]:<50} ({count:,} properties)")
        report.append("")
        
        report.append("Owner Type Distribution:")
        for owner_type, count in ownership['owner_type_distribution'].items():
            if stats['total_properties'] > 0:
                percentage = (count / stats['total_properties']) * 100
                report.append(f"  {owner_type}: {count:,} properties ({percentage:.1f}%)")
            else:
                report.append(f"  {owner_type}: {count:,} properties (0.0%)")
        report.append("")
        
        # Zoning Analysis
        report.append("🏗️ ZONING ANALYSIS")
        report.append("-" * 40)
        report.append("Top 10 Zoning Types by Count:")
        for i, (zone, count) in enumerate(list(zoning['zoning_distribution'].items())[:10], 1):
            report.append(f"  {i:2d}. {zone:<30} ({count:,} properties)")
        report.append("")
        
        report.append("Top 10 Zoning Types by Average Value:")
        for i, (zone, avg_value) in enumerate(list(zoning['top_zoning_by_value'].items())[:10], 1):
            report.append(f"  {i:2d}. {zone:<30} ${avg_value:,.0f}")
        report.append("")
        
        # Neighborhood Analysis
        report.append("🏘️ NEIGHBORHOOD ANALYSIS")
        report.append("-" * 40)
        report.append("Top 10 Neighborhoods by Average Value:")
        for i, (neigh, avg_value) in enumerate(list(neighborhood['top_neighborhoods_by_value'].items())[:10], 1):
            count = neighborhood['neighborhood_value_analysis'][neigh]['count']
            report.append(f"  {i:2d}. {neigh:<30} ${avg_value:,.0f} ({count:,} properties)")
        report.append("")
        
        # Value Analysis
        report.append("💰 VALUE DISTRIBUTION")
        report.append("-" * 40)
        for range_label, count in value['value_distribution'].items():
            if stats['properties_with_values'] > 0:
                percentage = (count / stats['properties_with_values']) * 100
                report.append(f"{range_label:<15}: {count:,} properties ({percentage:.1f}%)")
            else:
                report.append(f"{range_label:<15}: {count:,} properties (0.0%)")
        report.append("")
        
        # Year Built Analysis
        if 'year_built_statistics' in stats:
            year_stats = stats['year_built_statistics']
            report.append("📅 YEAR BUILT STATISTICS")
            report.append("-" * 40)
            report.append(f"Mean Year Built: {year_stats['mean_year']:.0f}")
            report.append(f"Median Year Built: {year_stats['median_year']:.0f}")
            report.append(f"Earliest Year: {year_stats['min_year']:.0f}")
            report.append(f"Latest Year: {year_stats['max_year']:.0f}")
            report.append("")
        
        report.append("=" * 80)
        report.append("END OF REPORT")
        report.append("=" * 80)
        
        # Save report to file
        with open('coral_gables_analysis_report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        # Print report to console
        print('\n'.join(report))
        print(f"\n✅ Analysis report saved as 'coral_gables_analysis_report.txt'")
        
        return report
    
    def run_complete_analysis(self):
        """Run the complete analysis pipeline"""
        print("🚀 Starting comprehensive Coral Gables property data analysis...")
        
        # Load and process data
        if not self.load_data():
            return False
        
        if not self.extract_property_data():
            return False
        
        # Run all analyses
        stats = self.basic_statistics()
        ownership = self.ownership_analysis()
        zoning = self.zoning_analysis()
        neighborhood = self.neighborhood_analysis()
        value = self.value_analysis()
        
        # Generate report
        self.generate_report(stats, ownership, zoning, neighborhood, value)
        
        print("\n🎉 Analysis complete! Check the generated file:")
        print("  - coral_gables_analysis_report.txt (detailed report)")
        
        return True

def main():
    """Main function to run the analysis"""
    analyzer = SimpleCoralGablesAnalyzer()
    analyzer.run_complete_analysis()

if __name__ == "__main__":
    main() 