#!/usr/bin/env python3
"""
Improved CGProperties GeoJSON Splitter
Uses the correct field names from the actual data structure
"""

import json
import os
import sys
import csv
from pathlib import Path
from typing import Dict, List, Any
import argparse
from datetime import datetime

class ImprovedCGPropertiesSplitter:
    def __init__(self, input_file: str = "CGProperties.json", output_dir: str = "split_output"):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.data = {}
        self.features = []
        
    def load_data(self):
        """Load the CGProperties GeoJSON file"""
        print(f"Loading data from {self.input_file}...")
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            # Extract features from GeoJSON
            if self.data.get('type') == 'FeatureCollection':
                self.features = self.data.get('features', [])
                print(f"Loaded {len(self.features)} features from GeoJSON FeatureCollection")
            else:
                print("Warning: File is not a GeoJSON FeatureCollection")
                self.features = []
                
        except FileNotFoundError:
            print(f"Error: File {self.input_file} not found")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON format - {e}")
            sys.exit(1)
    
    def get_property_value(self, feature, key, default="Unknown"):
        """Extract property value from GeoJSON feature"""
        properties = feature.get('properties', {})
        return properties.get(key, default)
    
    def split_by_owner(self):
        """Split data by owner using TRUE_OWNER field"""
        print("Splitting by owner...")
        owners = {}
        
        for feature in self.features:
            owner = self.get_property_value(feature, 'AddressPoints.TRUE_OWNER')
            if owner not in owners:
                owners[owner] = []
            owners[owner].append(feature)
        
        for owner, features in owners.items():
            # Clean owner name for filename
            clean_owner = str(owner).replace(' ', '_').replace('/', '_').replace('\\', '_')[:50]
            filename = f"owner_{clean_owner}.json"
            filepath = self.output_dir / filename
            
            # Create GeoJSON structure for this owner
            owner_geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(owner_geojson, f, indent=2, ensure_ascii=False)
            print(f"Created {filename} with {len(features)} features")
    
    def split_by_zoning(self):
        """Split data by zoning using PRIMARY_ZO field"""
        print("Splitting by zoning...")
        zoning_types = {}
        
        for feature in self.features:
            zoning = self.get_property_value(feature, 'AddressPoints.PRIMARY_ZO')
            if zoning not in zoning_types:
                zoning_types[zoning] = []
            zoning_types[zoning].append(feature)
        
        for zoning, features in zoning_types.items():
            clean_zoning = str(zoning).replace(' ', '_').replace('/', '_').replace('\\', '_')[:50]
            filename = f"zoning_{clean_zoning}.json"
            filepath = self.output_dir / filename
            
            zoning_geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(zoning_geojson, f, indent=2, ensure_ascii=False)
            print(f"Created {filename} with {len(features)} features")
    
    def split_by_trash_route(self):
        """Split data by trash route using Route field"""
        print("Splitting by trash route...")
        routes = {}
        
        for feature in self.features:
            route = self.get_property_value(feature, 'AddressPoints_AddSpatialJoin_10.Route')
            if route not in routes:
                routes[route] = []
            routes[route].append(feature)
        
        for route, features in routes.items():
            clean_route = str(route).replace(' ', '_').replace('/', '_').replace('\\', '_')[:50]
            filename = f"route_{clean_route}.json"
            filepath = self.output_dir / filename
            
            route_geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(route_geojson, f, indent=2, ensure_ascii=False)
            print(f"Created {filename} with {len(features)} features")
    
    def split_by_municipality(self):
        """Split data by municipality"""
        print("Splitting by municipality...")
        municipalities = {}
        
        for feature in self.features:
            municipality = self.get_property_value(feature, 'AddressPoints.MUNICIPALI')
            if municipality not in municipalities:
                municipalities[municipality] = []
            municipalities[municipality].append(feature)
        
        for municipality, features in municipalities.items():
            clean_municipality = str(municipality).replace(' ', '_').replace('/', '_').replace('\\', '_')[:50]
            filename = f"municipality_{clean_municipality}.json"
            filepath = self.output_dir / filename
            
            municipality_geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(municipality_geojson, f, indent=2, ensure_ascii=False)
            print(f"Created {filename} with {len(features)} features")
    
    def split_by_neighborhood(self):
        """Split data by neighborhood"""
        print("Splitting by neighborhood...")
        neighborhoods = {}
        
        for feature in self.features:
            neighborhood = self.get_property_value(feature, 'AddressPoints.NEIGHBORHO')
            if neighborhood not in neighborhoods:
                neighborhoods[neighborhood] = []
            neighborhoods[neighborhood].append(feature)
        
        for neighborhood, features in neighborhoods.items():
            clean_neighborhood = str(neighborhood).replace(' ', '_').replace('/', '_').replace('\\', '_')[:50]
            filename = f"neighborhood_{clean_neighborhood}.json"
            filepath = self.output_dir / filename
            
            neighborhood_geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(neighborhood_geojson, f, indent=2, ensure_ascii=False)
            print(f"Created {filename} with {len(features)} features")
    
    def split_by_chunk_size(self, chunk_size: int = 100):
        """Split data into chunks of specified size"""
        print(f"Splitting into chunks of {chunk_size} features...")
        
        for i in range(0, len(self.features), chunk_size):
            chunk = self.features[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            filename = f"chunk_{chunk_num:03d}.json"
            filepath = self.output_dir / filename
            
            chunk_geojson = {
                "type": "FeatureCollection",
                "features": chunk
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(chunk_geojson, f, indent=2, ensure_ascii=False)
            print(f"Created {filename} with {len(chunk)} features")
    
    def split_by_address_range(self, records_per_file: int = 50):
        """Split data by address ranges using TRUE_SITE_ field"""
        print(f"Splitting by address ranges ({records_per_file} features per file)...")
        
        # Sort by address for better organization
        sorted_features = sorted(self.features, key=lambda x: self.get_property_value(x, 'AddressPoints.TRUE_SITE_', ''))
        
        for i in range(0, len(sorted_features), records_per_file):
            chunk = sorted_features[i:i + records_per_file]
            file_num = i // records_per_file + 1
            
            # Get address range for filename
            start_addr = self.get_property_value(chunk[0], 'AddressPoints.TRUE_SITE_', 'Unknown')
            end_addr = self.get_property_value(chunk[-1], 'AddressPoints.TRUE_SITE_', 'Unknown')
            
            # Clean address strings for filename
            start_clean = str(start_addr).replace(' ', '_').replace('/', '_').replace('\\', '_')[:30]
            end_clean = str(end_addr).replace(' ', '_').replace('/', '_').replace('\\', '_')[:30]
            
            filename = f"address_range_{file_num:03d}_{start_clean}_to_{end_clean}.json"
            filepath = self.output_dir / filename
            
            address_geojson = {
                "type": "FeatureCollection",
                "features": chunk
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(address_geojson, f, indent=2, ensure_ascii=False)
            print(f"Created {filename} with {len(chunk)} features")
    
    def split_by_year_built(self):
        """Split data by year built"""
        print("Splitting by year built...")
        years = {}
        
        for feature in self.features:
            year = self.get_property_value(feature, 'AddressPoints.YEAR_BUILT')
            if year not in years:
                years[year] = []
            years[year].append(feature)
        
        for year, features in years.items():
            clean_year = str(year).replace(' ', '_').replace('/', '_').replace('\\', '_')[:20]
            filename = f"year_built_{clean_year}.json"
            filepath = self.output_dir / filename
            
            year_geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(year_geojson, f, indent=2, ensure_ascii=False)
            print(f"Created {filename} with {len(features)} features")
    
    def export_to_csv(self, filename: str = "properties.csv"):
        """Export all properties to CSV format"""
        print(f"Exporting all properties to {filename}...")
        
        if not self.features:
            print("No features to export")
            return
        
        # Get all possible field names from all features
        all_fields = set()
        for feature in self.features:
            properties = feature.get('properties', {})
            all_fields.update(properties.keys())
        
        # Sort fields for consistent output
        fieldnames = sorted(list(all_fields))
        
        filepath = self.output_dir / filename
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for feature in self.features:
                properties = feature.get('properties', {})
                # Ensure all fields are present (fill missing with empty string)
                row = {field: properties.get(field, '') for field in fieldnames}
                writer.writerow(row)
        
        print(f"Exported {len(self.features)} properties to {filename}")
    
    def export_split_to_csv(self, split_type: str, data_dict: Dict[str, List], filename_prefix: str):
        """Export a specific split to CSV format"""
        print(f"Exporting {split_type} split to CSV...")
        
        for key, features in data_dict.items():
            if not features:
                continue
                
            # Clean key for filename
            clean_key = str(key).replace(' ', '_').replace('/', '_').replace('\\', '_')[:50]
            filename = f"{filename_prefix}_{clean_key}.csv"
            filepath = self.output_dir / filename
            
            # Get all possible field names
            all_fields = set()
            for feature in features:
                properties = feature.get('properties', {})
                all_fields.update(properties.keys())
            
            fieldnames = sorted(list(all_fields))
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for feature in features:
                    properties = feature.get('properties', {})
                    row = {field: properties.get(field, '') for field in fieldnames}
                    writer.writerow(row)
            
            print(f"Created {filename} with {len(features)} features")
    
    def export_all_splits_to_csv(self):
        """Export all split types to CSV format"""
        print("Exporting all splits to CSV format...")
        
        # Export by owner
        owners = {}
        for feature in self.features:
            owner = self.get_property_value(feature, 'AddressPoints.TRUE_OWNER')
            if owner not in owners:
                owners[owner] = []
            owners[owner].append(feature)
        
        csv_dir = self.output_dir / "csv_exports"
        csv_dir.mkdir(exist_ok=True)
        original_output = self.output_dir
        self.output_dir = csv_dir
        
        self.export_split_to_csv("owner", owners, "owner")
        
        # Export by zoning
        zoning_types = {}
        for feature in self.features:
            zoning = self.get_property_value(feature, 'AddressPoints.PRIMARY_ZO')
            if zoning not in zoning_types:
                zoning_types[zoning] = []
            zoning_types[zoning].append(feature)
        
        self.export_split_to_csv("zoning", zoning_types, "zoning")
        
        # Export by route
        routes = {}
        for feature in self.features:
            route = self.get_property_value(feature, 'AddressPoints_AddSpatialJoin_10.Route')
            if route not in routes:
                routes[route] = []
            routes[route].append(feature)
        
        self.export_split_to_csv("route", routes, "route")
        
        # Export by municipality
        municipalities = {}
        for feature in self.features:
            municipality = self.get_property_value(feature, 'AddressPoints.MUNICIPALI')
            if municipality not in municipalities:
                municipalities[municipality] = []
            municipalities[municipality].append(feature)
        
        self.export_split_to_csv("municipality", municipalities, "municipality")
        
        # Export by neighborhood
        neighborhoods = {}
        for feature in self.features:
            neighborhood = self.get_property_value(feature, 'AddressPoints.NEIGHBORHO')
            if neighborhood not in neighborhoods:
                neighborhoods[neighborhood] = []
            neighborhoods[neighborhood].append(feature)
        
        self.export_split_to_csv("neighborhood", neighborhoods, "neighborhood")
        
        # Export by year built
        years = {}
        for feature in self.features:
            year = self.get_property_value(feature, 'AddressPoints.YEAR_BUILT')
            if year not in years:
                years[year] = []
            years[year].append(feature)
        
        self.export_split_to_csv("year_built", years, "year_built")
        
        # Export complete dataset
        self.export_to_csv("all_properties.csv")
        
        # Restore original output directory
        self.output_dir = original_output
        print(f"All CSV exports completed in '{csv_dir}' directory")
    
    def analyze_data_fields(self):
        """Analyze and display all available fields in the data"""
        print("Analyzing all available fields in the data...")
        
        if not self.features:
            print("No features loaded. Please load data first.")
            return
        
        # Collect all unique field names
        all_fields = set()
        field_values = {}
        
        for feature in self.features:
            properties = feature.get('properties', {})
            for field_name, value in properties.items():
                all_fields.add(field_name)
                if field_name not in field_values:
                    field_values[field_name] = set()
                field_values[field_name].add(str(value))
        
        # Sort fields for consistent output
        sorted_fields = sorted(list(all_fields))
        
        print(f"\nFound {len(sorted_fields)} unique fields in the data:")
        print("=" * 80)
        
        for field in sorted_fields:
            unique_values = len(field_values[field])
            sample_values = list(field_values[field])[:5]  # Show first 5 unique values
            print(f"\nField: {field}")
            print(f"  Unique values: {unique_values}")
            print(f"  Sample values: {sample_values}")
            
            # Check if this field is being used in our stories
            if any(field in story_field for story_field in [
                'AddressPoints.TRUE_SITE_', 'AddressPoints.TRUE_OWNER', 
                'AddressPoints.PRIMARY_ZO', 'AddressPoints.MUNICIPALI',
                'AddressPoints.NEIGHBORHO', 'AddressPoints.YEAR_BUILT',
                'AddressPoints_AddSpatialJoin_10.Route', 'AddressPoints.FLOOD_ZONE',
                'AddressPoints.PROP_TYPE', 'AddressPoints.SQ_FT', 'AddressPoints.UNITS',
                'AddressPoints.LAND_VALUE', 'AddressPoints.BUILDING_VALUE'
            ]):
                print(f"  ✓ Used in property stories")
            else:
                print(f"  ✗ NOT used in property stories")
        
        # Save field analysis to file
        analysis_file = self.output_dir / "field_analysis.json"
        analysis_data = {
            "total_features": len(self.features),
            "total_fields": len(sorted_fields),
            "fields": {}
        }
        
        for field in sorted_fields:
            analysis_data["fields"][field] = {
                "unique_values": len(field_values[field]),
                "sample_values": list(field_values[field])[:10],
                "used_in_stories": any(field in story_field for story_field in [
                    'AddressPoints.TRUE_SITE_', 'AddressPoints.TRUE_OWNER', 
                    'AddressPoints.PRIMARY_ZO', 'AddressPoints.MUNICIPALI',
                    'AddressPoints.NEIGHBORHO', 'AddressPoints.YEAR_BUILT',
                    'AddressPoints_AddSpatialJoin_10.Route', 'AddressPoints.FLOOD_ZONE',
                    'AddressPoints.PROP_TYPE', 'AddressPoints.SQ_FT', 'AddressPoints.UNITS',
                    'AddressPoints.LAND_VALUE', 'AddressPoints.BUILDING_VALUE'
                ])
            }
        
        with open(analysis_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_data, f, indent=2, ensure_ascii=False)
        
        print(f"\nField analysis saved to {analysis_file}")
    
    def generate_property_stories(self, output_file: str = "property_stories.jsonl"):
        """Generate natural language stories about each property for LLM training"""
        print(f"Generating property stories for LLM training...")
        
        filepath = self.output_dir / output_file
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for i, feature in enumerate(self.features):
                properties = feature.get('properties', {})
                
                # Extract key property information
                address = properties.get('AddressPoints.TRUE_SITE_', 'Unknown address')
                current_owner = properties.get('AddressPoints.TRUE_OWNER', 'Unknown owner')
                zoning = properties.get('AddressPoints.PRIMARY_ZO', 'Unknown zoning')
                municipality = properties.get('AddressPoints.MUNICIPALI', 'Unknown municipality')
                neighborhood = properties.get('AddressPoints.NEIGHBORHO', 'Unknown neighborhood')
                year_built = properties.get('AddressPoints.YEAR_BUILT', 'Unknown year')
                trash_route = properties.get('AddressPoints_AddSpatialJoin_10.Route', 'Unknown route')
                coordinates = feature.get('geometry', {}).get('coordinates', [])
                
                # Generate natural language story
                story = self._create_property_story(
                    address, current_owner, zoning, municipality, neighborhood,
                    year_built, trash_route, coordinates, properties
                )
                
                # Create training example for LLM
                training_example = {
                    "id": f"property_{i+1:06d}",
                    "address": address,
                    "story": story,
                    "metadata": {
                        "owner": current_owner,
                        "zoning": zoning,
                        "municipality": municipality,
                        "neighborhood": neighborhood,
                        "year_built": year_built,
                        "trash_route": trash_route,
                        "coordinates": coordinates
                    }
                }
                
                f.write(json.dumps(training_example, ensure_ascii=False) + '\n')
        
        print(f"Generated {len(self.features)} property stories in {output_file}")
    
    def _create_property_story(self, address, owner, zoning, municipality, neighborhood, 
                              year_built, trash_route, coordinates, all_properties):
        """Create a natural language story about a property"""
        
        story_parts = []
        
        # Helper function to safely check string values
        def safe_str(value):
            if value is None:
                return ""
            return str(value).strip()
        
        # Start with the address
        address_str = safe_str(address)
        if address_str and address_str != 'Unknown address':
            story_parts.append(f"The property located at {address_str}")
        else:
            story_parts.append("This property")
        
        # Add municipality and neighborhood context
        location_info = []
        municipality_str = safe_str(municipality)
        if municipality_str and municipality_str != 'Unknown municipality':
            location_info.append(f"in {municipality_str}")
        
        neighborhood_str = safe_str(neighborhood)
        if neighborhood_str and neighborhood_str != 'Unknown neighborhood':
            location_info.append(f"within the {neighborhood_str} neighborhood")
        
        if location_info:
            story_parts.append("is situated " + " ".join(location_info))
        
        # Add ownership information
        owner_str = safe_str(owner)
        if owner_str and owner_str != 'Unknown owner':
            story_parts.append(f"and is currently owned by {owner_str}")
        else:
            story_parts.append("and has no current owner listed")
        
        # Add zoning information
        zoning_str = safe_str(zoning)
        if zoning_str and zoning_str != 'Unknown zoning':
            story_parts.append(f"The property is zoned as {zoning_str}")
            
            # Add secondary zoning if available
            secondary_zoning = safe_str(all_properties.get('AddressPoints.SECONDARY_', ''))
            if secondary_zoning:
                story_parts.append(f"with secondary zoning of {secondary_zoning}")
        
        # Add lot size information
        lot_size = safe_str(all_properties.get('AddressPoints.LOT_SIZE', ''))
        if lot_size:
            try:
                size = float(lot_size)
                story_parts.append(f"The lot size is {size:,.0f} square feet")
            except (ValueError, TypeError):
                story_parts.append(f"The lot size is recorded as {lot_size} square feet")
        
        # Add year built information
        year_built_str = safe_str(year_built)
        if year_built_str and year_built_str != 'Unknown year':
            try:
                year = int(year_built_str)
                story_parts.append(f"The building was constructed in {year}")
            except (ValueError, TypeError):
                story_parts.append(f"The building construction year is recorded as {year_built_str}")
        
        # Add trash collection information
        trash_route_str = safe_str(trash_route)
        if trash_route_str and trash_route_str != 'Unknown route':
            story_parts.append(f"Trash collection is handled via route {trash_route_str}")
        
        # Add recycling information
        recycling = safe_str(all_properties.get('AddressPoints.Recycling', ''))
        if recycling:
            story_parts.append(f"Recycling is scheduled for {recycling}")
        
        # Add garbage collection days
        garbage_days = safe_str(all_properties.get('AddressPoints.GarbageR', ''))
        if garbage_days:
            story_parts.append(f"Garbage collection occurs on {garbage_days}")
        
        # Add property type/use
        prop_type = safe_str(all_properties.get('AddressPoints.PROP_TYPE', ''))
        if prop_type:
            story_parts.append(f"The property type is {prop_type}")
        
        # Add building square footage
        sq_ft = safe_str(all_properties.get('AddressPoints.SQ_FT', ''))
        if sq_ft:
            story_parts.append(f"The building size is {sq_ft} square feet")
        
        # Add number of units
        units = safe_str(all_properties.get('AddressPoints.UNITS', ''))
        if units:
            story_parts.append(f"The property has {units} units")
        
        # Add condo flag
        condo_flag = safe_str(all_properties.get('AddressPoints.CONDO_FLAG', ''))
        if condo_flag == 'Y':
            story_parts.append("This is a condominium property")
        
        # Add historical designation
        historical = safe_str(all_properties.get('AddressPoints.Historical', ''))
        if historical == 'Yes':
            story_parts.append("This property has historical designation")
        
        # Add tax information
        total_value = safe_str(all_properties.get('AddressPoints.TOTAL_VAL1', ''))
        if total_value:
            try:
                value = float(total_value)
                story_parts.append(f"The total property value is ${value:,.0f}")
            except (ValueError, TypeError):
                story_parts.append(f"The total property value is recorded as {total_value}")
        
        # Add land value
        land_value = safe_str(all_properties.get('AddressPoints.LAND_VAL_2', ''))
        if land_value:
            try:
                value = float(land_value)
                story_parts.append(f"The land value is ${value:,.0f}")
            except (ValueError, TypeError):
                story_parts.append(f"The land value is recorded as {land_value}")
        
        # Add building value
        building_value = safe_str(all_properties.get('AddressPoints.BUILDING_V', ''))
        if building_value:
            try:
                value = float(building_value)
                story_parts.append(f"The building value is ${value:,.0f}")
            except (ValueError, TypeError):
                story_parts.append(f"The building value is recorded as {building_value}")
        
        # Add mailing address if different from site address
        mailing_address = safe_str(all_properties.get('AddressPoints.TRUE_MAILI', ''))
        if mailing_address and mailing_address != address_str:
            story_parts.append(f"The mailing address is {mailing_address}")
        
        # Add coordinates if available
        if coordinates and len(coordinates) >= 2:
            try:
                lat, lon = coordinates[1], coordinates[0]  # GeoJSON format is [lon, lat]
                story_parts.append(f"The property is located at coordinates {lat:.6f}, {lon:.6f}")
            except (IndexError, TypeError):
                pass
        
        # Add legal description (shortened)
        legal = safe_str(all_properties.get('AddressPoints.LEGAL', ''))
        if legal:
            # Truncate long legal descriptions
            legal_short = legal[:100] + "..." if len(legal) > 100 else legal
            story_parts.append(f"The legal description is {legal_short}")
        
        # Combine all parts into a coherent story
        story = ". ".join(story_parts) + "."
        
        return story
    
    def generate_qa_pairs(self, output_file: str = "property_qa_pairs.jsonl"):
        """Generate question-answer pairs for LLM training"""
        print(f"Generating Q&A pairs for LLM training...")
        
        filepath = self.output_dir / output_file
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for i, feature in enumerate(self.features):
                properties = feature.get('properties', {})
                address = properties.get('AddressPoints.TRUE_SITE_', 'Unknown address')
                
                # Generate the property story
                story = self._create_property_story(
                    properties.get('AddressPoints.TRUE_SITE_', 'Unknown address'),
                    properties.get('AddressPoints.TRUE_OWNER', 'Unknown owner'),
                    properties.get('AddressPoints.PRIMARY_ZO', 'Unknown zoning'),
                    properties.get('AddressPoints.MUNICIPALI', 'Unknown municipality'),
                    properties.get('AddressPoints.NEIGHBORHO', 'Unknown neighborhood'),
                    properties.get('AddressPoints.YEAR_BUILT', 'Unknown year'),
                    properties.get('AddressPoints_AddSpatialJoin_10.Route', 'Unknown route'),
                    feature.get('geometry', {}).get('coordinates', []),
                    properties
                )
                
                # Create various question-answer pairs
                qa_pairs = [
                    {
                        "question": f"Tell me about the property at {address}",
                        "answer": story
                    },
                    {
                        "question": f"Who owns the property at {address}?",
                        "answer": f"The property at {address} is owned by {properties.get('AddressPoints.TRUE_OWNER', 'an unknown owner')}."
                    },
                    {
                        "question": f"What is the zoning for {address}?",
                        "answer": f"The property at {address} is zoned as {properties.get('AddressPoints.PRIMARY_ZO', 'unknown zoning')}."
                    },
                    {
                        "question": f"Is {address} in a flood zone?",
                        "answer": self._get_flood_zone_answer(address, properties)
                    },
                    {
                        "question": f"What trash route serves {address}?",
                        "answer": f"The property at {address} is served by trash route {properties.get('AddressPoints_AddSpatialJoin_10.Route', 'an unknown route')}."
                    }
                ]
                
                for qa in qa_pairs:
                    training_example = {
                        "id": f"qa_{i+1:06d}",
                        "address": address,
                        "question": qa["question"],
                        "answer": qa["answer"],
                        "context": story
                    }
                    f.write(json.dumps(training_example, ensure_ascii=False) + '\n')
        
        print(f"Generated Q&A pairs in {output_file}")
    
    def generate_rag_chunks(self, output_file: str = "rag_chunks.jsonl", chunk_size: int = 1000):
        """Generate RAG-optimized chunks for vector database storage"""
        print(f"Generating RAG-optimized chunks...")
        
        # Helper function to safely check string values
        def safe_str(value):
            if value is None:
                return ""
            return str(value).strip()
        
        filepath = self.output_dir / output_file
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for i, feature in enumerate(self.features):
                properties = feature.get('properties', {})
                
                # Extract key information
                address = safe_str(properties.get('AddressPoints.TRUE_SITE_', ''))
                owner = safe_str(properties.get('AddressPoints.TRUE_OWNER', ''))
                zoning = safe_str(properties.get('AddressPoints.PRIMARY_ZO', ''))
                municipality = safe_str(properties.get('AddressPoints.MUNICIPALI', ''))
                neighborhood = safe_str(properties.get('AddressPoints.NEIGHBORHO', ''))
                year_built = safe_str(properties.get('AddressPoints.YEAR_BUILT', ''))
                trash_route = safe_str(properties.get('AddressPoints_AddSpatialJoin_10.Route', ''))
                coordinates = feature.get('geometry', {}).get('coordinates', [])
                
                # Create searchable content
                searchable_content = f"""
Property Address: {address}
Owner: {owner}
Zoning: {zoning}
Municipality: {municipality}
Neighborhood: {neighborhood}
Year Built: {year_built}
Trash Route: {trash_route}
"""
                
                # Add additional searchable fields
                additional_fields = [
                    ('Lot Size', properties.get('AddressPoints.LOT_SIZE', '')),
                    ('Property Type', properties.get('AddressPoints.PROP_TYPE', '')),
                    ('Building Value', properties.get('AddressPoints.BUILDING_V', '')),
                    ('Land Value', properties.get('AddressPoints.LAND_VAL_2', '')),
                    ('Total Value', properties.get('AddressPoints.TOTAL_VAL1', '')),
                    ('Recycling', properties.get('AddressPoints.Recycling', '')),
                    ('Garbage Days', properties.get('AddressPoints.GarbageR', '')),
                    ('Legal Description', properties.get('AddressPoints.LEGAL', '')[:200]),
                    ('Mailing Address', properties.get('AddressPoints.TRUE_MAILI', '')),
                    ('Folio Number', properties.get('AddressPoints.FOLIO', ''))
                ]
                
                for field_name, value in additional_fields:
                    if value and str(value).strip():
                        searchable_content += f"{field_name}: {value}\n"
                
                # Create RAG chunk
                rag_chunk = {
                    "id": f"property_{i+1:06d}",
                    "content": searchable_content.strip(),
                    "metadata": {
                        "address": address,
                        "owner": owner,
                        "zoning": zoning,
                        "municipality": municipality,
                        "neighborhood": neighborhood,
                        "year_built": year_built,
                        "trash_route": trash_route,
                        "coordinates": coordinates,
                        "property_id": properties.get('AddressPoints.FID', ''),
                        "folio": properties.get('AddressPoints.FOLIO', ''),
                        "type": "property_data"
                    },
                    "embedding_text": f"{address} {owner} {zoning} {municipality} {neighborhood} {year_built} {trash_route}"
                }
                
                f.write(json.dumps(rag_chunk, ensure_ascii=False) + '\n')
        
        print(f"Generated {len(self.features)} RAG chunks in {output_file}")
    
    def generate_search_queries(self, output_file: str = "search_queries.jsonl"):
        """Generate common search queries for testing RAG system"""
        print(f"Generating search queries...")
        
        # Helper function to safely check string values
        def safe_str(value):
            if value is None:
                return ""
            return str(value).strip()
        
        filepath = self.output_dir / output_file
        
        # Common property search patterns
        search_patterns = [
            "Who owns the property at {address}?",
            "What is the zoning for {address}?",
            "What trash route serves {address}?",
            "When was {address} built?",
            "What is the property value of {address}?",
            "Is {address} in a flood zone?",
            "What neighborhood is {address} in?",
            "Who owns properties in {neighborhood}?",
            "What properties does {owner} own?",
            "Show me properties with zoning {zoning}",
            "What properties are served by trash route {route}?",
            "Find properties built in {year}",
            "What is the lot size of {address}?",
            "Does {address} have historical designation?",
            "What is the mailing address for {address}?"
        ]
        
        with open(filepath, 'w', encoding='utf-8') as f:
            query_id = 1
            
            # Generate queries for specific properties
            for feature in self.features[:100]:  # Sample first 100 properties
                properties = feature.get('properties', {})
                address = safe_str(properties.get('AddressPoints.TRUE_SITE_', ''))
                owner = safe_str(properties.get('AddressPoints.TRUE_OWNER', ''))
                zoning = safe_str(properties.get('AddressPoints.PRIMARY_ZO', ''))
                neighborhood = safe_str(properties.get('AddressPoints.NEIGHBORHO', ''))
                year_built = safe_str(properties.get('AddressPoints.YEAR_BUILT', ''))
                trash_route = safe_str(properties.get('AddressPoints_AddSpatialJoin_10.Route', ''))
                
                if address and address.strip():
                    for pattern in search_patterns[:5]:  # Use first 5 patterns per property
                        try:
                            query = pattern.format(
                                address=address,
                                owner=owner,
                                zoning=zoning,
                                neighborhood=neighborhood,
                                year=year_built,
                                route=trash_route
                            )
                            
                            search_query = {
                                "id": f"query_{query_id:06d}",
                                "query": query,
                                "expected_address": address,
                                "query_type": "property_specific"
                            }
                            
                            f.write(json.dumps(search_query, ensure_ascii=False) + '\n')
                            query_id += 1
                        except KeyError:
                            continue
            
            # Generate general search queries
            general_queries = [
                "Find all properties owned by the City of Coral Gables",
                "Show me all properties with historical designation",
                "List all condominium properties",
                "Find properties with lot size over 10,000 square feet",
                "Show me properties with total value over $1 million",
                "Find all properties in flood zones",
                "List properties built before 1950",
                "Show me properties with multiple units",
                "Find properties with specific zoning codes",
                "List all properties served by specific trash routes"
            ]
            
            for query in general_queries:
                search_query = {
                    "id": f"query_{query_id:06d}",
                    "query": query,
                    "query_type": "general_search"
                }
                
                f.write(json.dumps(search_query, ensure_ascii=False) + '\n')
                query_id += 1
        
        print(f"Generated {query_id-1} search queries in {output_file}")
    
    def _get_flood_zone_answer(self, address, properties):
        """Generate answer about flood zone status"""
        flood_zone = properties.get('AddressPoints.FLOOD_ZONE', '')
        if flood_zone and flood_zone.strip():
            if flood_zone.upper() in ['YES', 'TRUE', '1', 'FLOOD']:
                return f"Yes, the property at {address} is located in a flood zone."
            else:
                return f"No, the property at {address} is not in a flood zone."
        else:
            return f"Flood zone information is not available for the property at {address}."
    
    def create_summary_report(self):
        """Create a summary report of the data"""
        print("Creating summary report...")
        
        # Get all unique values
        owners = set(self.get_property_value(f, 'AddressPoints.TRUE_OWNER') for f in self.features)
        zoning_types = set(self.get_property_value(f, 'AddressPoints.PRIMARY_ZO') for f in self.features)
        routes = set(self.get_property_value(f, 'AddressPoints_AddSpatialJoin_10.Route') for f in self.features)
        municipalities = set(self.get_property_value(f, 'AddressPoints.MUNICIPALI') for f in self.features)
        neighborhoods = set(self.get_property_value(f, 'AddressPoints.NEIGHBORHO') for f in self.features)
        years = set(self.get_property_value(f, 'AddressPoints.YEAR_BUILT') for f in self.features)
        
        summary = {
            "total_features": len(self.features),
            "unique_owners": len(owners),
            "unique_zoning_types": len(zoning_types),
            "unique_trash_routes": len(routes),
            "unique_municipalities": len(municipalities),
            "unique_neighborhoods": len(neighborhoods),
            "unique_years_built": len(years),
            "zoning_distribution": {},
            "owner_distribution": {},
            "route_distribution": {},
            "municipality_distribution": {},
            "neighborhood_distribution": {},
            "year_built_distribution": {},
            "available_properties": [],
            "generated_at": datetime.now().isoformat()
        }
        
        # Calculate distributions
        for feature in self.features:
            zoning = self.get_property_value(feature, 'AddressPoints.PRIMARY_ZO')
            owner = self.get_property_value(feature, 'AddressPoints.TRUE_OWNER')
            route = self.get_property_value(feature, 'AddressPoints_AddSpatialJoin_10.Route')
            municipality = self.get_property_value(feature, 'AddressPoints.MUNICIPALI')
            neighborhood = self.get_property_value(feature, 'AddressPoints.NEIGHBORHO')
            year = self.get_property_value(feature, 'AddressPoints.YEAR_BUILT')
            
            summary["zoning_distribution"][zoning] = summary["zoning_distribution"].get(zoning, 0) + 1
            summary["owner_distribution"][owner] = summary["owner_distribution"].get(owner, 0) + 1
            summary["route_distribution"][route] = summary["route_distribution"].get(route, 0) + 1
            summary["municipality_distribution"][municipality] = summary["municipality_distribution"].get(municipality, 0) + 1
            summary["neighborhood_distribution"][neighborhood] = summary["neighborhood_distribution"].get(neighborhood, 0) + 1
            summary["year_built_distribution"][year] = summary["year_built_distribution"].get(year, 0) + 1
        
        # Get available property keys from first feature
        if self.features:
            summary["available_properties"] = list(self.features[0].get('properties', {}).keys())
        
        # Save summary
        summary_file = self.output_dir / "summary_report.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        print(f"Created summary_report.json")
    
    def run_all_splits(self, chunk_size: int = 100, records_per_file: int = 50):
        """Run all splitting methods"""
        print("Starting comprehensive data split...")
        self.load_data()
        
        # Create subdirectories for different split types
        (self.output_dir / "by_owner").mkdir(exist_ok=True)
        (self.output_dir / "by_zoning").mkdir(exist_ok=True)
        (self.output_dir / "by_route").mkdir(exist_ok=True)
        (self.output_dir / "by_municipality").mkdir(exist_ok=True)
        (self.output_dir / "by_neighborhood").mkdir(exist_ok=True)
        (self.output_dir / "by_year_built").mkdir(exist_ok=True)
        (self.output_dir / "by_chunks").mkdir(exist_ok=True)
        (self.output_dir / "by_address").mkdir(exist_ok=True)
        
        # Change to subdirectories for each split type
        original_output = self.output_dir
        
        # Split by owner
        self.output_dir = original_output / "by_owner"
        self.split_by_owner()
        
        # Split by zoning
        self.output_dir = original_output / "by_zoning"
        self.split_by_zoning()
        
        # Split by trash route
        self.output_dir = original_output / "by_route"
        self.split_by_trash_route()
        
        # Split by municipality
        self.output_dir = original_output / "by_municipality"
        self.split_by_municipality()
        
        # Split by neighborhood
        self.output_dir = original_output / "by_neighborhood"
        self.split_by_neighborhood()
        
        # Split by year built
        self.output_dir = original_output / "by_year_built"
        self.split_by_year_built()
        
        # Split by chunks
        self.output_dir = original_output / "by_chunks"
        self.split_by_chunk_size(chunk_size)
        
        # Split by address range
        self.output_dir = original_output / "by_address"
        self.split_by_address_range(records_per_file)
        
        # Create summary report in main output directory
        self.output_dir = original_output
        self.create_summary_report()
        
        # Export all splits to CSV
        self.export_all_splits_to_csv()
        
        # Generate property stories
        self.generate_property_stories()
        self.generate_qa_pairs()
        
        print(f"\nAll splits completed! Check the '{original_output}' directory for results.")

def main():
    parser = argparse.ArgumentParser(description='Split CGProperties GeoJSON file into smaller files')
    parser.add_argument('--input', '-i', default='CGProperties.json', 
                       help='Input JSON file path')
    parser.add_argument('--output', '-o', default='split_output',
                       help='Output directory')
    parser.add_argument('--chunk-size', '-c', type=int, default=100,
                       help='Number of records per chunk file')
    parser.add_argument('--records-per-file', '-r', type=int, default=50,
                       help='Number of records per address range file')
    parser.add_argument('--method', '-m', choices=['owner', 'zoning', 'route', 'municipality', 'neighborhood', 'year_built', 'chunks', 'address', 'csv', 'stories', 'qa_pairs', 'rag_chunks', 'search_queries', 'analyze', 'all'],
                       default='all', help='Splitting method to use')
    parser.add_argument('--csv-only', action='store_true',
                       help='Export only to CSV format (no JSON splits)')
    parser.add_argument('--llm-training', action='store_true',
                       help='Generate LLM training data (stories and Q&A pairs)')
    
    args = parser.parse_args()
    
    splitter = ImprovedCGPropertiesSplitter(args.input, args.output)
    
    if args.method == 'all':
        if args.csv_only:
            splitter.load_data()
            splitter.export_all_splits_to_csv()
        else:
            splitter.run_all_splits(args.chunk_size, args.records_per_file)
    else:
        splitter.load_data()
        
        if args.method == 'owner':
            splitter.split_by_owner()
        elif args.method == 'zoning':
            splitter.split_by_zoning()
        elif args.method == 'route':
            splitter.split_by_trash_route()
        elif args.method == 'municipality':
            splitter.split_by_municipality()
        elif args.method == 'neighborhood':
            splitter.split_by_neighborhood()
        elif args.method == 'year_built':
            splitter.split_by_year_built()
        elif args.method == 'chunks':
            splitter.split_by_chunk_size(args.chunk_size)
        elif args.method == 'address':
            splitter.split_by_address_range(args.records_per_file)
        elif args.method == 'csv':
            splitter.export_all_splits_to_csv()
        elif args.method == 'stories':
            splitter.generate_property_stories()
        elif args.method == 'qa_pairs':
            splitter.generate_qa_pairs()
        elif args.method == 'rag_chunks':
            splitter.generate_rag_chunks()
        elif args.method == 'search_queries':
            splitter.generate_search_queries()
        elif args.method == 'analyze':
            splitter.analyze_data_fields()
        
        if args.method not in ['csv', 'stories', 'qa_pairs', 'rag_chunks', 'search_queries', 'analyze']:
            splitter.create_summary_report()
        
        # Generate LLM training data if requested
        if args.llm_training:
            splitter.generate_property_stories()
            splitter.generate_qa_pairs()

if __name__ == "__main__":
    main() 