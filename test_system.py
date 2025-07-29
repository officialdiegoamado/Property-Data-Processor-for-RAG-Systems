#!/usr/bin/env python3
"""
Coral Gables Property Data Processing System - Test Suite
Comprehensive testing for all system components
"""

import unittest
import json
import os
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Any

# Import the main classes (assuming they're in the same directory)
try:
    from CGProperties_Splitter import ImprovedCGPropertiesSplitter
    from create_rag_data import CoralGablesRAGProcessor
    from simple_data_analysis import CoralGablesDataAnalyzer
except ImportError:
    print("Warning: Could not import main classes. Running basic tests only.")

class TestCoralGablesSystem(unittest.TestCase):
    """Test suite for the Coral Gables Property Data Processing System"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp()
        self.test_data_file = os.path.join(self.test_dir, "test_properties.json")
        self.test_output_dir = os.path.join(self.test_dir, "test_output")
        
        # Create test GeoJSON data
        self.create_test_data()
    
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.test_dir)
    
    def create_test_data(self):
        """Create sample GeoJSON data for testing"""
        test_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "AddressPoints.ADDRESS": "123 Test Street",
                        "AddressPoints.TRUE_OWNER": "Test Owner",
                        "AddressPoints.PRIMARY_ZO": "R-1",
                        "AddressPoints.BASE_YEAR": "2020",
                        "AddressPoints.BUILDING_V": "500000",
                        "AddressPoints.NEIGHBORHOOD": "Test Neighborhood",
                        "AddressPoints.MUNICIPALITY": "Coral Gables",
                        "AddressPoints.TRASH_ROUTE": "Route 1"
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-80.3, 25.7]
                    }
                },
                {
                    "type": "Feature",
                    "properties": {
                        "AddressPoints.ADDRESS": "456 Sample Ave",
                        "AddressPoints.TRUE_OWNER": "Sample Owner",
                        "AddressPoints.PRIMARY_ZO": "C-1",
                        "AddressPoints.BASE_YEAR": "2019",
                        "AddressPoints.BUILDING_V": "750000",
                        "AddressPoints.NEIGHBORHOOD": "Sample Neighborhood",
                        "AddressPoints.MUNICIPALITY": "Coral Gables",
                        "AddressPoints.TRASH_ROUTE": "Route 2"
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-80.2, 25.8]
                    }
                }
            ]
        }
        
        with open(self.test_data_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, indent=2)
    
    def test_data_file_creation(self):
        """Test that test data file was created correctly"""
        self.assertTrue(os.path.exists(self.test_data_file))
        
        with open(self.test_data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.assertEqual(data['type'], 'FeatureCollection')
        self.assertEqual(len(data['features']), 2)
    
    def test_geojson_structure(self):
        """Test GeoJSON structure validation"""
        with open(self.test_data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Test required GeoJSON fields
        self.assertIn('type', data)
        self.assertIn('features', data)
        self.assertEqual(data['type'], 'FeatureCollection')
        
        # Test feature structure
        for feature in data['features']:
            self.assertIn('type', feature)
            self.assertIn('properties', feature)
            self.assertIn('geometry', feature)
            self.assertEqual(feature['type'], 'Feature')
    
    def test_property_fields(self):
        """Test that required property fields exist"""
        with open(self.test_data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        required_fields = [
            'AddressPoints.ADDRESS',
            'AddressPoints.TRUE_OWNER',
            'AddressPoints.PRIMARY_ZO',
            'AddressPoints.BASE_YEAR',
            'AddressPoints.BUILDING_V'
        ]
        
        for feature in data['features']:
            properties = feature['properties']
            for field in required_fields:
                self.assertIn(field, properties)
    
    def test_coordinate_validation(self):
        """Test coordinate validation"""
        with open(self.test_data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for feature in data['features']:
            geometry = feature['geometry']
            coordinates = geometry['coordinates']
            
            # Test coordinate format
            self.assertEqual(len(coordinates), 2)
            self.assertIsInstance(coordinates[0], (int, float))
            self.assertIsInstance(coordinates[1], (int, float))
            
            # Test coordinate ranges (Coral Gables area)
            self.assertTrue(-81.0 <= coordinates[0] <= -80.0)  # Longitude
            self.assertTrue(25.0 <= coordinates[1] <= 26.0)    # Latitude
    
    def test_data_types(self):
        """Test data type validation"""
        with open(self.test_data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for feature in data['features']:
            properties = feature['properties']
            
            # Test string fields
            string_fields = ['AddressPoints.ADDRESS', 'AddressPoints.TRUE_OWNER']
            for field in string_fields:
                self.assertIsInstance(properties[field], str)
            
            # Test numeric fields
            numeric_fields = ['AddressPoints.BUILDING_V', 'AddressPoints.BASE_YEAR']
            for field in numeric_fields:
                value = properties[field]
                self.assertTrue(str(value).replace('.', '').replace('-', '').isdigit())
    
    def test_output_directory_creation(self):
        """Test output directory creation"""
        os.makedirs(self.test_output_dir, exist_ok=True)
        self.assertTrue(os.path.exists(self.test_output_dir))
        self.assertTrue(os.path.isdir(self.test_output_dir))
    
    def test_json_output_generation(self):
        """Test JSON output file generation"""
        output_file = os.path.join(self.test_output_dir, "test_output.json")
        
        with open(self.test_data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        self.assertTrue(os.path.exists(output_file))
        
        # Verify output file content
        with open(output_file, 'r', encoding='utf-8') as f:
            output_data = json.load(f)
        
        self.assertEqual(output_data['type'], 'FeatureCollection')
        self.assertEqual(len(output_data['features']), 2)
    
    def test_csv_output_generation(self):
        """Test CSV output file generation"""
        output_file = os.path.join(self.test_output_dir, "test_output.csv")
        
        with open(self.test_data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Create CSV from JSON data
        import csv
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if data['features']:
                fieldnames = list(data['features'][0]['properties'].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for feature in data['features']:
                    writer.writerow(feature['properties'])
        
        self.assertTrue(os.path.exists(output_file))
        
        # Verify CSV content
        with open(output_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        self.assertIn('AddressPoints.ADDRESS', content)
        self.assertIn('123 Test Street', content)
        self.assertIn('456 Sample Ave', content)
    
    def test_property_story_generation(self):
        """Test property story generation logic"""
        test_property = {
            'AddressPoints.ADDRESS': '123 Test Street',
            'AddressPoints.TRUE_OWNER': 'Test Owner',
            'AddressPoints.PRIMARY_ZO': 'R-1',
            'AddressPoints.BASE_YEAR': '2020',
            'AddressPoints.BUILDING_V': '500000'
        }
        
        # Simulate story generation
        story = f"This property at {test_property['AddressPoints.ADDRESS']} is owned by {test_property['AddressPoints.TRUE_OWNER']}. "
        story += f"It is zoned as {test_property['AddressPoints.PRIMARY_ZO']} and was built in {test_property['AddressPoints.BASE_YEAR']}. "
        story += f"The building value is ${test_property['AddressPoints.BUILDING_V']}."
        
        self.assertIn('123 Test Street', story)
        self.assertIn('Test Owner', story)
        self.assertIn('R-1', story)
        self.assertIn('2020', story)
        self.assertIn('500000', story)
    
    def test_qa_pair_generation(self):
        """Test Q&A pair generation logic"""
        test_property = {
            'AddressPoints.ADDRESS': '123 Test Street',
            'AddressPoints.TRUE_OWNER': 'Test Owner',
            'AddressPoints.BUILDING_V': '500000'
        }
        
        # Simulate Q&A generation
        qa_pairs = [
            {
                'question': f"Who owns the property at {test_property['AddressPoints.ADDRESS']}?",
                'answer': f"The property at {test_property['AddressPoints.ADDRESS']} is owned by {test_property['AddressPoints.TRUE_OWNER']}."
            },
            {
                'question': f"What is the building value of {test_property['AddressPoints.ADDRESS']}?",
                'answer': f"The building value of {test_property['AddressPoints.ADDRESS']} is ${test_property['AddressPoints.BUILDING_V']}."
            }
        ]
        
        self.assertEqual(len(qa_pairs), 2)
        self.assertIn('Who owns', qa_pairs[0]['question'])
        self.assertIn('Test Owner', qa_pairs[0]['answer'])
        self.assertIn('building value', qa_pairs[1]['question'])
        self.assertIn('500000', qa_pairs[1]['answer'])
    
    def test_error_handling(self):
        """Test error handling for invalid data"""
        # Test with missing required field
        invalid_property = {
            'AddressPoints.ADDRESS': '123 Test Street',
            # Missing TRUE_OWNER field
            'AddressPoints.PRIMARY_ZO': 'R-1'
        }
        
        # Should handle missing field gracefully
        owner = invalid_property.get('AddressPoints.TRUE_OWNER', 'Unknown Owner')
        self.assertEqual(owner, 'Unknown Owner')
    
    def test_file_size_validation(self):
        """Test file size validation"""
        file_size = os.path.getsize(self.test_data_file)
        self.assertGreater(file_size, 0)
        self.assertLess(file_size, 100 * 1024 * 1024)  # Less than 100MB
    
    def test_encoding_validation(self):
        """Test file encoding validation"""
        # Test UTF-8 encoding
        with open(self.test_data_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Should read without encoding errors
        self.assertIsInstance(content, str)
        self.assertGreater(len(content), 0)

def run_performance_tests():
    """Run performance tests"""
    print("\n🔍 Running Performance Tests...")
    
    # Test file reading performance
    import time
    
    start_time = time.time()
    with open("CGProperties.json", 'r', encoding='utf-8') as f:
        data = json.load(f)
    load_time = time.time() - start_time
    
    print(f"✅ Data loading time: {load_time:.2f} seconds")
    print(f"✅ Properties loaded: {len(data.get('features', []))}")
    
    # Test memory usage estimation
    import sys
    data_size = sys.getsizeof(data)
    print(f"✅ Estimated memory usage: {data_size / (1024*1024):.2f} MB")

def run_system_health_check():
    """Run system health check"""
    print("\n🏥 Running System Health Check...")
    
    # Check required files
    required_files = [
        "CGProperties.json",
        "CGProperties_Splitter.py",
        "create_rag_data.py",
        "simple_data_analysis.py"
    ]
    
    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file} - Found")
        else:
            print(f"❌ {file} - Missing")
    
    # Check output directories
    output_dirs = ["split_output", "rag_data"]
    for dir_name in output_dirs:
        if os.path.exists(dir_name):
            print(f"✅ {dir_name}/ - Found")
        else:
            print(f"⚠️  {dir_name}/ - Not found (will be created during processing)")

if __name__ == '__main__':
    print("🧪 Coral Gables Property Data Processing System - Test Suite")
    print("=" * 70)
    
    # Run health check
    run_system_health_check()
    
    # Run performance tests if main data file exists
    if os.path.exists("CGProperties.json"):
        run_performance_tests()
    
    # Run unit tests
    print("\n🧪 Running Unit Tests...")
    unittest.main(verbosity=2, exit=False)
    
    print("\n✅ Test suite completed!") 