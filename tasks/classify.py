import os
import pandas as pd
import numpy as np
from PIL import Image
import logging
from typing import Dict, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class ImageClassifier:
    """Image classifier that categorizes images based on resolution"""
    
    def __init__(self):
        self.resolution_categories = {
            '240p': (320, 240),
            '480p': (854, 480), 
            '720p': (1280, 720),
            '1080p': (1920, 1080),
            '4K': (3840, 2160)
        }
    
    def get_image_resolution(self, image_path: str) -> Tuple[int, int]:
        """Extract image resolution using PIL"""
        try:
            with Image.open(image_path) as img:
                width, height = img.size
                return width, height
        except Exception as e:
            logger.error(f"Error reading image {image_path}: {e}")
            return 0, 0
    
    def classify_resolution(self, width: int, height: int) -> str:
        """Classify image based on resolution"""
        total_pixels = width * height
        
        # Define pixel thresholds for each category
        if total_pixels <= 76800:  # 320x240
            return '240p'
        elif total_pixels <= 409920:  # 854x480
            return '480p'
        elif total_pixels <= 921600:  # 1280x720
            return '720p'
        elif total_pixels <= 2073600:  # 1920x1080
            return '1080p'
        else:
            return '4K'
    
    def process_images(self, input_folder: str) -> pd.DataFrame:
        """Process all images in the input folder and return classification results"""
        results = []
        
        if not os.path.exists(input_folder):
            logger.error(f"Input folder {input_folder} does not exist")
            return pd.DataFrame()
        
        # Get all image files
        image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.webp'}
        image_files = []
        
        for file in os.listdir(input_folder):
            if any(file.lower().endswith(ext) for ext in image_extensions):
                image_files.append(file)
        
        logger.info(f"Found {len(image_files)} images to process")
        
        for filename in image_files:
            image_path = os.path.join(input_folder, filename)
            
            # Get image resolution
            width, height = self.get_image_resolution(image_path)
            
            if width > 0 and height > 0:
                # Classify resolution
                resolution_category = self.classify_resolution(width, height)
                
                # Get file size
                file_size = os.path.getsize(image_path)
                
                # Calculate aspect ratio
                aspect_ratio = round(width / height, 2)
                
                results.append({
                    'filename': filename,
                    'width': width,
                    'height': height,
                    'resolution_category': resolution_category,
                    'total_pixels': width * height,
                    'aspect_ratio': aspect_ratio,
                    'file_size_bytes': file_size,
                    'file_size_mb': round(file_size / (1024 * 1024), 2),
                    'processed_at': datetime.now().isoformat()
                })
                
                logger.info(f"Processed {filename}: {width}x{height} -> {resolution_category}")
            else:
                logger.warning(f"Could not process {filename}")
        
        return pd.DataFrame(results)
    
    def generate_statistics(self, df: pd.DataFrame) -> Dict:
        """Generate statistics from the classification results"""
        if df.empty:
            return {}
        
        stats = {
            'total_images': len(df),
            'resolution_distribution': df['resolution_category'].value_counts().to_dict(),
            'avg_file_size_mb': round(df['file_size_mb'].mean(), 2),
            'total_size_mb': round(df['file_size_mb'].sum(), 2),
            'avg_aspect_ratio': round(df['aspect_ratio'].mean(), 2),
            'resolution_summary': {
                'min_resolution': f"{df['width'].min()}x{df['height'].min()}",
                'max_resolution': f"{df['width'].max()}x{df['height'].max()}",
                'avg_resolution': f"{int(df['width'].mean())}x{int(df['height'].mean())}"
            }
        }
        
        return stats

def classify_images_task(input_folder: str, output_file: str = None) -> Dict:
    """
    Main task function for image classification
    
    Args:
        input_folder: Path to folder containing images
        output_file: Optional path to save results CSV
    
    Returns:
        Dictionary containing classification results and statistics
    """
    classifier = ImageClassifier()
    
    # Process images
    logger.info(f"Starting image classification for folder: {input_folder}")
    results_df = classifier.process_images(input_folder)
    
    if results_df.empty:
        logger.warning("No images were processed successfully")
        return {'status': 'error', 'message': 'No images processed'}
    
    # Generate statistics
    stats = classifier.generate_statistics(results_df)
    
    # Save results if output file specified
    if output_file:
        results_df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
    
    # Prepare return data
    result_data = {
        'status': 'success',
        'total_images': len(results_df),
        'statistics': stats,
        'results': results_df.to_dict('records')
    }
    
    logger.info(f"Classification completed. Processed {len(results_df)} images.")
    return result_data

if __name__ == "__main__":
    # Test the classifier
    import sys
    
    if len(sys.argv) > 1:
        input_folder = sys.argv[1]
        result = classify_images_task(input_folder, "classification_results.csv")
        print(f"Classification result: {result}")
    else:
        print("Usage: python classify.py <input_folder>")
