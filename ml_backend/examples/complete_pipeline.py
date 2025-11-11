"""
Example: Complete collapse detection pipeline
Demonstrates how to use all components together
"""

import numpy as np
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import our modules
from src.validation_engine import DiversityAnalyzer, StratificationConfig
from src.collapse_engine import (
    CollapseDetector, CollapseConfig,
    SignatureLibrary,
    CollapseLocalizer, LocalizationConfig,
    RecommendationEngine
)


async def main():
    """Complete example pipeline"""
    
    logger.info("=" * 80)
    logger.info("SYNTHOS ML VALIDATION PIPELINE - COMPLETE EXAMPLE")
    logger.info("=" * 80)
    
    # ==================== STEP 1: Generate Synthetic Test Data ====================
    logger.info("\n📊 Step 1: Generating synthetic test data...")
    
    # Original data (ground truth)
    n_samples = 10000
    n_features = 100
    
    original_data = np.random.randn(n_samples, n_features)
    logger.info(f"Original data: {original_data.shape}")
    
    # Synthetic data (simulate collapse by reducing variance)
    synthetic_data = original_data * 0.7 + np.random.randn(n_samples, n_features) * 0.3
    logger.info(f"Synthetic data: {synthetic_data.shape}")
    
    # ==================== STEP 2: Diversity Analysis ====================
    logger.info("\n🔍 Step 2: Analyzing dataset diversity...")
    
    analyzer = DiversityAnalyzer(StratificationConfig(
        target_sample_size=5000,
        use_gpu=True
    ))
    
    # Note: In production, you'd analyze from file path
    # diversity_score = await analyzer.analyze_diversity(
    #     data_path="s3://bucket/dataset.parquet",
    #     data_format="parquet"
    # )
    
    # For demo, we'll compute basic stats
    logger.info("✅ Diversity analysis complete (simulated)")
    logger.info(f"   - Sample size: {n_samples:,}")
    logger.info(f"   - Features: {n_features}")
    logger.info(f"   - Diversity score: 75.3/100")
    
    # ==================== STEP 3: Collapse Detection ====================
    logger.info("\n🚨 Step 3: Multi-dimensional collapse detection...")
    
    detector = CollapseDetector(CollapseConfig(
        use_gpu=True
    ))
    
    collapse_result = await detector.detect_collapse(
        synthetic_data=synthetic_data,
        original_data=original_data,
        model_gradients=None,  # Optional
        training_metrics=None  # Optional
    )
    
    logger.info(f"✅ Collapse detection complete!")
    logger.info(f"   - Overall score: {collapse_result.overall_score:.1f}/100")
    logger.info(f"   - Collapse detected: {collapse_result.collapse_detected}")
    logger.info(f"   - Confidence: {collapse_result.confidence:.1f}%")
    
    logger.info(f"\n   Dimension Scores:")
    for name, dim in collapse_result.dimensions.items():
        status_icon = "✅" if dim.passed else "❌"
        logger.info(f"   {status_icon} {dim.name}: {dim.score:.1f}/100 (threshold: {dim.threshold})")
    
    logger.info(f"\n   Warnings:")
    for warning in collapse_result.warnings:
        logger.info(f"   {warning}")
    
    # ==================== STEP 4: Signature Library ====================
    logger.info("\n📚 Step 4: Checking signature library for similar patterns...")
    
    sig_library = SignatureLibrary(use_gpu=True)
    
    # Add current pattern to library
    data_statistics = {
        'mean': np.mean(synthetic_data, axis=0).tolist(),
        'std': np.std(synthetic_data, axis=0).tolist()
    }
    
    signature_id = sig_library.add_signature(
        dataset_id="example_dataset_001",
        dimension_scores={name: dim.score for name, dim in collapse_result.dimensions.items()},
        collapse_score=collapse_result.overall_score,
        data_statistics=data_statistics,
        metadata={'source': 'example', 'version': '1.0'}
    )
    
    logger.info(f"✅ Added signature: {signature_id}")
    
    # Search for similar patterns
    similar_patterns = await sig_library.find_similar_patterns(
        dimension_scores={name: dim.score for name, dim in collapse_result.dimensions.items()},
        data_statistics=data_statistics,
        top_k=3
    )
    
    logger.info(f"   Found {len(similar_patterns)} similar patterns")
    for i, match in enumerate(similar_patterns, 1):
        logger.info(f"   {i}. Dataset: {match.dataset_id}, Similarity: {match.similarity:.2f}")
    
    # ==================== STEP 5: Localization ====================
    if collapse_result.collapse_detected:
        logger.info("\n🎯 Step 5: Localizing problematic data rows...")
        
        localizer = CollapseLocalizer(LocalizationConfig(
            top_k=100,
            use_gpu=True
        ))
        
        localization_result = await localizer.localize_collapse(
            data=synthetic_data,
            collapse_dimensions={name: dim.score for name, dim in collapse_result.dimensions.items()},
            model=None,  # Optional
            gradients=None  # Optional
        )
        
        logger.info(f"✅ Localization complete!")
        logger.info(f"   - Problematic rows: {localization_result.total_problematic:,} ({localization_result.percentage_problematic:.1f}%)")
        logger.info(f"   - Top 5 most problematic:")
        for idx, score in localization_result.top_k_rows[:5]:
            logger.info(f"     - Row {idx}: impact score {score:.3f}")
        
        logger.info(f"\n   Recommendations:")
        for rec in localization_result.recommendations:
            logger.info(f"   {rec}")
    else:
        localization_result = None
        logger.info("\n✅ No collapse detected - skipping localization")
    
    # ==================== STEP 6: Generate Recommendations ====================
    logger.info("\n💡 Step 6: Generating actionable recommendations...")
    
    recommender = RecommendationEngine()
    
    recommendations = await recommender.generate_recommendations(
        collapse_score=collapse_result.overall_score,
        dimension_scores={name: dim.score for name, dim in collapse_result.dimensions.items()},
        localization_results=localization_result,
        dataset_size=n_samples,
        budget_usd=5000  # $5K budget
    )
    
    logger.info(f"✅ Generated {len(recommendations.recommendations)} recommendations")
    logger.info(f"\n   Summary:")
    logger.info(recommendations.summary)
    
    logger.info(f"\n   Top 3 Priority Recommendations:")
    for i, rec in enumerate(recommendations.recommendations[:3], 1):
        logger.info(f"\n   {i}. {rec.title} ({rec.priority.name})")
        logger.info(f"      Impact: +{rec.estimated_impact:.1f} points")
        logger.info(f"      Effort: {rec.effort_hours:.1f} hours")
        logger.info(f"      Cost: ${rec.cost_usd:.0f}")
        logger.info(f"      Description: {rec.description}")
    
    logger.info(f"\n   Total Impact: +{recommendations.total_estimated_impact:.1f} points")
    logger.info(f"   Total Effort: {recommendations.total_effort_hours:.1f} hours")
    logger.info(f"   Total Cost: ${recommendations.total_cost_usd:.0f}")
    logger.info(f"   Projected Final Score: {min(100, collapse_result.overall_score + recommendations.total_estimated_impact):.1f}/100")
    
    # ==================== FINAL SUMMARY ====================
    logger.info("\n" + "=" * 80)
    logger.info("PIPELINE EXECUTION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"\n✅ All 6 steps completed successfully!")
    logger.info(f"\n📊 Final Assessment:")
    logger.info(f"   - Current Score: {collapse_result.overall_score:.1f}/100")
    logger.info(f"   - Collapse Detected: {collapse_result.collapse_detected}")
    logger.info(f"   - Problematic Rows: {localization_result.total_problematic if localization_result else 0:,}")
    logger.info(f"   - Recommendations: {len(recommendations.recommendations)}")
    logger.info(f"   - Projected Score: {min(100, collapse_result.overall_score + recommendations.total_estimated_impact):.1f}/100")
    
    if collapse_result.collapse_detected:
        logger.info(f"\n🚨 ACTION REQUIRED: Review recommendations and apply fixes before training")
    else:
        logger.info(f"\n✅ DATASET APPROVED: Quality is sufficient for training")
    
    logger.info("\n" + "=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
