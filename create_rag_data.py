#!/usr/bin/env python3
"""
Coral Gables Property Data Processor for RAG Systems
Converts large JSON property data into storytelling-ready text files for LLM ingestion
Enhanced with coordinate conversion and rich narrative storytelling
"""

import json
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Tuple

class CoralGablesRAGProcessor:
    def __init__(self, input_file: str = "CGProperties.json"):
        self.input_file = input_file
        self.output_dir = "rag_data"
        self.properties = []
        
        # Coral Gables neighborhoods and landmarks for context
        self.neighborhoods = {
            "coral_gables": "Coral Gables",
            "coconut_grove": "Coconut Grove", 
            "south_miami": "South Miami",
            "pinecrest": "Pinecrest",
            "key_biscayne": "Key Biscayne"
        }
        
        # Common property types and descriptions
        self.property_types = {
            "residential": "residential home",
            "commercial": "commercial property", 
            "industrial": "industrial facility",
            "vacant": "vacant lot",
            "mixed_use": "mixed-use property"
        }
        
    def load_data(self) -> bool:
        """Load the large JSON file and extract properties"""
        try:
            print(f"📁 Loading data from {self.input_file}...")
            
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(data, list):
                self.properties = data
            elif isinstance(data, dict) and 'properties' in data:
                self.properties = data['properties']
            else:
                print("❌ Unexpected JSON structure")
                return False
                
            print(f"✅ Loaded {len(self.properties)} properties")
            return True
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def create_output_directory(self):
        """Create output directory for RAG files"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"📁 Created output directory: {self.output_dir}")
    
    def convert_coordinates_to_location_description(self, property_data: Dict[str, Any]) -> str:
        """Convert coordinates and address data into meaningful location descriptions"""
        
        # Extract coordinate information
        x_coord = property_data.get('AddressPoints_AddSpatialJoin_10', {}).get('X_COORD')
        y_coord = property_data.get('AddressPoints_AddSpatialJoin_10', {}).get('Y_COORD')
        
        # Extract address information
        address = property_data.get('AddressPoints', {}).get('ADDRESS', 'Unknown Address')
        street_name = property_data.get('AddressPoints', {}).get('STREET_NAME', '')
        street_number = property_data.get('AddressPoints', {}).get('STREET_NUMBER', '')
        
        # Create location description
        location_desc = f"Located at {address}"
        
        if street_name and street_number:
            location_desc += f", specifically at {street_number} {street_name}"
        
        # Add coordinate context if available
        if x_coord and y_coord:
            try:
                x_float = float(x_coord)
                y_float = float(y_coord)
                
                # Determine general area based on coordinates
                if -80.3 <= x_float <= -80.2 and 25.7 <= y_float <= 25.8:
                    location_desc += " in the heart of Coral Gables"
                elif -80.3 <= x_float <= -80.2 and 25.6 <= y_float <= 25.7:
                    location_desc += " in the southern area of Coral Gables"
                elif -80.2 <= x_float <= -80.1 and 25.7 <= y_float <= 25.8:
                    location_desc += " in the eastern section of Coral Gables"
                else:
                    location_desc += f" at coordinates ({x_coord}, {y_coord})"
                    
            except (ValueError, TypeError):
                location_desc += f" at coordinates ({x_coord}, {y_coord})"
        
        return location_desc
    
    def extract_property_context(self, property_data: Dict[str, Any]) -> str:
        """Extract additional context for storytelling"""
        
        # Extract various property attributes
        zoning = property_data.get('AddressPoints', {}).get('ZONING', '')
        land_use = property_data.get('AddressPoints', {}).get('LAND_USE', '')
        year_built = property_data.get('AddressPoints', {}).get('YEAR_BUILT', '')
        square_footage = property_data.get('AddressPoints', {}).get('SQ_FT', '')
        
        context_parts = []
        
        # Add zoning context
        if zoning:
            if 'residential' in zoning.lower():
                context_parts.append("This is a residential property")
            elif 'commercial' in zoning.lower():
                context_parts.append("This commercial property")
            elif 'industrial' in zoning.lower():
                context_parts.append("This industrial facility")
            else:
                context_parts.append(f"This property is zoned as {zoning}")
        
        # Add year built context
        if year_built and year_built != '0':
            try:
                year = int(year_built)
                if year > 0:
                    if year < 1950:
                        context_parts.append(f"built in {year}, making it a historic property")
                    elif year < 1980:
                        context_parts.append(f"constructed in {year}")
                    else:
                        context_parts.append(f"built in {year}")
            except (ValueError, TypeError):
                pass
        
        # Add square footage context
        if square_footage and square_footage != '0':
            try:
                sqft = int(square_footage)
                if sqft > 0:
                    if sqft < 1000:
                        context_parts.append("a cozy property")
                    elif sqft < 3000:
                        context_parts.append("a comfortable-sized property")
                    else:
                        context_parts.append("a spacious property")
            except (ValueError, TypeError):
                pass
        
        return " ".join(context_parts) if context_parts else "This property"
    
    def create_community_context(self, property_data: Dict[str, Any]) -> str:
        """Create community and neighborhood context"""
        
        address = property_data.get('AddressPoints', {}).get('ADDRESS', '').lower()
        
        # Determine neighborhood based on address patterns
        if 'miracle mile' in address or 'ponce de leon' in address:
            return "This property is part of the vibrant Miracle Mile district, known for its shopping, dining, and cultural attractions."
        elif 'biltmore' in address or 'granada' in address:
            return "Located in the prestigious Biltmore area, known for its historic architecture and luxury properties."
        elif 'gables by the sea' in address or 'snapper creek' in address:
            return "This property is in the scenic Gables by the Sea area, offering beautiful waterfront views and a peaceful atmosphere."
        elif 'coral way' in address:
            return "Situated along the historic Coral Way, a tree-lined boulevard connecting Coral Gables to Miami."
        else:
            return "This property is part of the beautiful Coral Gables community, known for its Mediterranean architecture, tree-lined streets, and excellent municipal services."
    
    def extract_property_story(self, property_data: Dict[str, Any]) -> str:
        """Convert a property record into a rich storytelling narrative"""
        
        # Extract key information
        address = property_data.get('AddressPoints', {}).get('ADDRESS', 'Unknown Address')
        owner = property_data.get('AddressPoints', {}).get('GRANTEE_1', 'Unknown Owner')
        legal_desc = property_data.get('AddressPoints', {}).get('LEGAL', 'No legal description available')
        trash_route = property_data.get('AddressPoints', {}).get('Trash', 'No trash route information')
        recycling = property_data.get('AddressPoints', {}).get('Recycling', 'No recycling information')
        garbage_days = property_data.get('AddressPoints', {}).get('GarbageR', 'No garbage schedule')
        
        # Enhanced location description
        location_desc = self.convert_coordinates_to_location_description(property_data)
        
        # Property context
        property_context = self.extract_property_context(property_data)
        
        # Community context
        community_context = self.create_community_context(property_data)
        
        # Create a rich narrative story
        story = f"""
PROPERTY STORY - CORAL GABLES, FLORIDA

{location_desc}

{property_context} is owned by {owner}. {community_context}

The legal description of this property reads: {legal_desc}

Municipal Services and Community Benefits:
- Trash Collection Route: {trash_route}
- Recycling Schedule: {recycling} 
- Garbage Pickup Days: {garbage_days}

This property benefits from Coral Gables' comprehensive municipal services, including excellent waste management, well-maintained streets, and a strong sense of community. The city is known for its commitment to preserving its unique character while providing modern amenities and services to all residents and property owners.

The property is part of a community that values both historic preservation and modern convenience, making it an integral part of the vibrant Coral Gables landscape.

---
"""
        return story
    
    def create_story_chunks(self, chunk_size: int = 100):
        """Create story chunks for RAG ingestion"""
        print(f"📝 Creating story chunks (max {chunk_size} properties per file)...")
        
        total_properties = len(self.properties)
        num_chunks = (total_properties + chunk_size - 1) // chunk_size
        
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_properties)
            chunk_properties = self.properties[start_idx:end_idx]
            
            # Create story file
            filename = f"coral_gables_properties_chunk_{i+1:03d}.txt"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"CORAL GABLES PROPERTY STORIES - CHUNK {i+1}\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Properties {start_idx+1} to {end_idx} of {total_properties}\n")
                f.write("=" * 80 + "\n\n")
                
                for j, prop in enumerate(chunk_properties, start_idx + 1):
                    f.write(f"PROPERTY #{j}\n")
                    f.write(self.extract_property_story(prop))
                    f.write("\n")
            
            print(f"  📄 Created: {filename} ({len(chunk_properties)} properties)")
    
    def create_summary_file(self):
        """Create a summary file with key statistics"""
        print("📊 Creating summary file...")
        
        summary_file = os.path.join(self.output_dir, "coral_gables_summary.txt")
        
        # Count unique owners
        owners = set()
        addresses = set()
        neighborhoods = set()
        
        for prop in self.properties:
            owner = prop.get('AddressPoints', {}).get('GRANTEE_1', 'Unknown')
            address = prop.get('AddressPoints', {}).get('ADDRESS', 'Unknown')
            if owner and owner != 'Unknown':
                owners.add(owner)
            if address and address != 'Unknown':
                addresses.add(address)
                # Extract neighborhood hints
                address_lower = address.lower()
                if 'miracle mile' in address_lower:
                    neighborhoods.add('Miracle Mile')
                elif 'biltmore' in address_lower:
                    neighborhoods.add('Biltmore Area')
                elif 'coral way' in address_lower:
                    neighborhoods.add('Coral Way')
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("CORAL GABLES PROPERTY DATABASE SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Total Properties: {len(self.properties):,}\n")
            f.write(f"Unique Addresses: {len(addresses):,}\n")
            f.write(f"Unique Property Owners: {len(owners):,}\n")
            f.write(f"Identified Neighborhoods: {', '.join(neighborhoods) if neighborhoods else 'Various areas'}\n")
            f.write(f"Data Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("This dataset contains comprehensive property information for Coral Gables, Florida,\n")
            f.write("including ownership details, municipal services, geographic coordinates, and rich\n")
            f.write("contextual information for storytelling. The data is structured for use in RAG\n")
            f.write("(Retrieval-Augmented Generation) systems to enable AI-powered storytelling about\n")
            f.write("Coral Gables properties, neighborhoods, and community life.\n\n")
            
            f.write("Each property story includes:\n")
            f.write("- Location descriptions with coordinate context\n")
            f.write("- Property ownership and legal information\n")
            f.write("- Municipal services and community benefits\n")
            f.write("- Neighborhood context and historical significance\n")
            f.write("- Rich narrative elements for engaging storytelling\n")
        
        print(f"  📄 Created: coral_gables_summary.txt")
    
    def create_search_index(self):
        """Create a search index for quick property lookup"""
        print("🔍 Creating search index...")
        
        index_file = os.path.join(self.output_dir, "property_search_index.txt")
        
        with open(index_file, 'w', encoding='utf-8') as f:
            f.write("CORAL GABLES PROPERTY SEARCH INDEX\n")
            f.write("=" * 40 + "\n\n")
            f.write("Format: Address | Owner | Chunk File | Property Number | Neighborhood Context\n")
            f.write("-" * 100 + "\n\n")
            
            chunk_size = 100
            for i, prop in enumerate(self.properties):
                chunk_num = (i // chunk_size) + 1
                prop_num = i + 1
                
                address = prop.get('AddressPoints', {}).get('ADDRESS', 'Unknown')
                owner = prop.get('AddressPoints', {}).get('GRANTEE_1', 'Unknown')
                chunk_file = f"coral_gables_properties_chunk_{chunk_num:03d}.txt"
                
                # Add neighborhood context
                address_lower = address.lower()
                if 'miracle mile' in address_lower:
                    neighborhood = "Miracle Mile District"
                elif 'biltmore' in address_lower:
                    neighborhood = "Biltmore Area"
                elif 'coral way' in address_lower:
                    neighborhood = "Coral Way"
                else:
                    neighborhood = "Coral Gables"
                
                f.write(f"{address} | {owner} | {chunk_file} | Property #{prop_num} | {neighborhood}\n")
        
        print(f"  📄 Created: property_search_index.txt")
    
    def process_all(self, chunk_size: int = 100):
        """Process all data and create RAG-ready files"""
        print("╔══════════════════════════════════════════════════════════════╗")
        print("║              CORAL GABLES RAG DATA PROCESSOR                ║")
        print("║                    For LLM Storytelling                     ║")
        print("║              Enhanced with Coordinate Conversion            ║")
        print("╚══════════════════════════════════════════════════════════════╝")
        print()
        
        # Load data
        if not self.load_data():
            return False
        
        # Create output directory
        self.create_output_directory()
        
        # Process data
        self.create_story_chunks(chunk_size)
        self.create_summary_file()
        self.create_search_index()
        
        print()
        print("╔══════════════════════════════════════════════════════════════╗")
        print("║                    PROCESSING COMPLETE                      ║")
        print("╠══════════════════════════════════════════════════════════════╣")
        print("║                                                              ║")
        print("║  ✅ RAG data created successfully!                          ║")
        print("║  📁 Output directory: rag_data/                             ║")
        print("║  📄 Story chunks ready for LLM ingestion                    ║")
        print("║  🗺️  Coordinates converted to location descriptions          ║")
        print("║  📖 Rich narrative context added                            ║")
        print("║  🔍 Search index for quick property lookup                  ║")
        print("║  📊 Summary file with key statistics                        ║")
        print("║                                                              ║")
        print("║  Your RAG system can now generate compelling stories        ║")
        print("║  about Coral Gables properties with rich context!          ║")
        print("║                                                              ║")
        print("╚══════════════════════════════════════════════════════════════╝")
        
        return True

def main():
    """Main function to run the RAG processor"""
    processor = CoralGablesRAGProcessor()
    
    # Ask user for chunk size
    try:
        chunk_size = int(input("Enter chunk size (properties per file, default 100): ") or "100")
    except ValueError:
        chunk_size = 100
    
    # Process the data
    success = processor.process_all(chunk_size)
    
    if success:
        print("\n🚀 Ready for RAG system integration!")
        print("💡 Your LLM can now tell rich stories about Coral Gables properties!")
        print("🗺️  Coordinates have been converted to meaningful location descriptions.")
    else:
        print("\n❌ Processing failed. Please check your data file.")

if __name__ == "__main__":
    main() 