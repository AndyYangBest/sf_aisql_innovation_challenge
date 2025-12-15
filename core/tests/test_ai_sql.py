"""Unit tests for AI SQL API endpoints."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.app.api.v1.ai_sql import (
    ai_aggregate,
    ai_classify,
    ai_complete,
    ai_filter,
    ai_sentiment,
    ai_transcribe,
    extract_structured_data,
    semantic_join,
    summarize,
)
from src.app.schemas.ai_sql import (
    AIAggregateRequest,
    AIClassifyRequest,
    AICompleteRequest,
    AIFilterRequest,
    AISentimentRequest,
    AITranscribeRequest,
    ExtractStructuredDataRequest,
    SemanticJoinRequest,
    SummarizeRequest,
)


@pytest.fixture
def mock_ai_sql_service():
    """Mock AI SQL service."""
    return Mock()


class TestAIComplete:
    """Test AI_COMPLETE endpoint."""

    @pytest.mark.asyncio
    async def test_ai_complete_success(self, mock_ai_sql_service):
        """Test successful AI completion."""
        request = AICompleteRequest(
            model="claude-3-7-sonnet",
            prompt="Summarize this data",
            response_format=None
        )

        mock_ai_sql_service.ai_complete = AsyncMock(return_value="Generated summary")

        with patch("src.app.api.v1.ai_sql.get_ai_sql_service", return_value=mock_ai_sql_service):
            result = await ai_complete(request, mock_ai_sql_service)

            assert result.response == "Generated summary"
            mock_ai_sql_service.ai_complete.assert_called_once_with(
                model="claude-3-7-sonnet",
                prompt="Summarize this data",
                response_format=None
            )


class TestAITranscribe:
    """Test AI_TRANSCRIBE endpoint."""

    @pytest.mark.asyncio
    async def test_ai_transcribe_success(self, mock_ai_sql_service):
        """Test successful audio transcription."""
        request = AITranscribeRequest(
            audio_file_column="audio_file",
            table_name="audio_table"
        )

        mock_results = [
            {"transcription_text": "Hello world", "audio_duration_seconds": 5.2},
            {"transcription_text": "Test audio", "audio_duration_seconds": 3.1}
        ]
        mock_ai_sql_service.ai_transcribe = AsyncMock(return_value=mock_results)

        with patch("src.app.api.v1.ai_sql.get_ai_sql_service", return_value=mock_ai_sql_service):
            result = await ai_transcribe(request, mock_ai_sql_service)

            assert len(result.results) == 2
            assert result.results[0]["transcription_text"] == "Hello world"
            mock_ai_sql_service.ai_transcribe.assert_called_once()


class TestAIClassify:
    """Test AI_CLASSIFY endpoint."""

    @pytest.mark.asyncio
    async def test_ai_classify_success(self, mock_ai_sql_service):
        """Test successful classification."""
        request = AIClassifyRequest(
            content_column="content",
            categories=["positive", "negative", "neutral"],
            table_name="reviews",
            prompt_prefix="Classify sentiment: "
        )

        mock_results = [
            {"primary_label": "positive", "primary_confidence": 0.95},
            {"primary_label": "negative", "primary_confidence": 0.87}
        ]
        mock_ai_sql_service.ai_classify = AsyncMock(return_value=mock_results)

        with patch("src.app.api.v1.ai_sql.get_ai_sql_service", return_value=mock_ai_sql_service):
            result = await ai_classify(request, mock_ai_sql_service)

            assert len(result.results) == 2
            assert result.results[0]["primary_label"] == "positive"
            mock_ai_sql_service.ai_classify.assert_called_once()


class TestAIFilter:
    """Test AI_FILTER endpoint."""

    @pytest.mark.asyncio
    async def test_ai_filter_success(self, mock_ai_sql_service):
        """Test successful AI filtering."""
        request = AIFilterRequest(
            filter_condition="Is this a complaint? {0}",
            table_name="customer_feedback",
            columns=["feedback_text", "customer_id"]
        )

        mock_results = [
            {"feedback_text": "Product is broken", "customer_id": 123},
            {"feedback_text": "Not working", "customer_id": 456}
        ]
        mock_ai_sql_service.ai_filter = AsyncMock(return_value=mock_results)

        with patch("src.app.api.v1.ai_sql.get_ai_sql_service", return_value=mock_ai_sql_service):
            result = await ai_filter(request, mock_ai_sql_service)

            assert len(result.results) == 2
            assert result.results[0]["customer_id"] == 123
            mock_ai_sql_service.ai_filter.assert_called_once()


class TestAIAggregate:
    """Test AI_AGG endpoint."""

    @pytest.mark.asyncio
    async def test_ai_aggregate_success(self, mock_ai_sql_service):
        """Test successful AI aggregation."""
        request = AIAggregateRequest(
            column_to_aggregate="feedback",
            aggregation_prompt="Summarize main issues reported",
            table_name="support_tickets",
            group_by="month"
        )

        mock_results = [
            {"total_records": 150, "insights": "Main issues: login problems, slow performance"},
            {"total_records": 120, "insights": "Main issues: crashes, data loss"}
        ]
        mock_ai_sql_service.ai_aggregate = AsyncMock(return_value=mock_results)

        with patch("src.app.api.v1.ai_sql.get_ai_sql_service", return_value=mock_ai_sql_service):
            result = await ai_aggregate(request, mock_ai_sql_service)

            assert len(result.results) == 2
            assert result.results[0]["total_records"] == 150
            mock_ai_sql_service.ai_aggregate.assert_called_once()


class TestAISentiment:
    """Test AI_SENTIMENT endpoint."""

    @pytest.mark.asyncio
    async def test_ai_sentiment_success(self, mock_ai_sql_service):
        """Test successful sentiment analysis."""
        request = AISentimentRequest(
            text_column="review_text",
            table_name="product_reviews"
        )

        mock_results = [
            {"primary_sentiment": "positive", "sentiment_score": 0.92},
            {"primary_sentiment": "negative", "sentiment_score": 0.85}
        ]
        mock_ai_sql_service.ai_sentiment = AsyncMock(return_value=mock_results)

        with patch("src.app.api.v1.ai_sql.get_ai_sql_service", return_value=mock_ai_sql_service):
            result = await ai_sentiment(request, mock_ai_sql_service)

            assert len(result.results) == 2
            assert result.results[0]["primary_sentiment"] == "positive"
            mock_ai_sql_service.ai_sentiment.assert_called_once()


class TestSummarize:
    """Test SUMMARIZE endpoint."""

    @pytest.mark.asyncio
    async def test_summarize_success(self, mock_ai_sql_service):
        """Test successful summarization."""
        request = SummarizeRequest(
            text_column="article_content",
            table_name="news_articles"
        )

        mock_results = [
            {"summary": "Article discusses AI trends in 2024"},
            {"summary": "Report on climate change impacts"}
        ]
        mock_ai_sql_service.summarize = AsyncMock(return_value=mock_results)

        with patch("src.app.api.v1.ai_sql.get_ai_sql_service", return_value=mock_ai_sql_service):
            result = await summarize(request, mock_ai_sql_service)

            assert len(result.results) == 2
            assert "AI trends" in result.results[0]["summary"]
            mock_ai_sql_service.summarize.assert_called_once()


class TestSemanticJoin:
    """Test semantic JOIN endpoint."""

    @pytest.mark.asyncio
    async def test_semantic_join_success(self, mock_ai_sql_service):
        """Test successful semantic join."""
        request = SemanticJoinRequest(
            left_table="customer_issues",
            right_table="solution_articles",
            left_column="issue_description",
            right_column="solution_text",
            join_condition="Can {1} solve {0}?"
        )

        mock_results = [
            {"left_content": "Login not working", "right_content": "Reset password steps"},
            {"left_content": "Slow performance", "right_content": "Optimize settings guide"}
        ]
        mock_ai_sql_service.semantic_join = AsyncMock(return_value=mock_results)

        with patch("src.app.api.v1.ai_sql.get_ai_sql_service", return_value=mock_ai_sql_service):
            result = await semantic_join(request, mock_ai_sql_service)

            assert len(result.results) == 2
            assert "Login not working" in result.results[0]["left_content"]
            mock_ai_sql_service.semantic_join.assert_called_once()


class TestExtractStructuredData:
    """Test structured data extraction endpoint."""

    @pytest.mark.asyncio
    async def test_extract_structured_data_success(self, mock_ai_sql_service):
        """Test successful data extraction."""
        request = ExtractStructuredDataRequest(
            text_column="email_body",
            table_name="customer_emails",
            extraction_prompt="Extract contact info",
            schema={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "email": {"type": "string"}
                }
            }
        )

        mock_results = [
            {"extracted_data": {"name": "John Doe", "email": "john@example.com"}},
            {"extracted_data": {"name": "Jane Smith", "email": "jane@example.com"}}
        ]
        mock_ai_sql_service.extract_structured_data = AsyncMock(return_value=mock_results)

        with patch("src.app.api.v1.ai_sql.get_ai_sql_service", return_value=mock_ai_sql_service):
            result = await extract_structured_data(request, mock_ai_sql_service)

            assert len(result.results) == 2
            assert result.results[0]["extracted_data"]["name"] == "John Doe"
            mock_ai_sql_service.extract_structured_data.assert_called_once()
