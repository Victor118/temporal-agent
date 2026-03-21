package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

func RegisterWebTools(r *Registry) {
	client := &http.Client{Timeout: 30 * time.Second}

	r.Register(&Tool{
		Name:        "web_fetch",
		Description: "Fetch a web page and return its text content (HTML is stripped). Use web_search first to find relevant URLs.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"url": {"type": "string", "description": "URL to fetch"}
			},
			"required": ["url"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct{ URL string `json:"url"` }
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}

			req, err := http.NewRequestWithContext(ctx, "GET", params.URL, nil)
			if err != nil {
				return "", fmt.Errorf("web_fetch: %w", err)
			}
			req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; TemporalAgent/1.0)")

			resp, err := client.Do(req)
			if err != nil {
				return "", fmt.Errorf("web_fetch: %w", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(io.LimitReader(resp.Body, 200_000))
			if err != nil {
				return "", fmt.Errorf("web_fetch: read body: %w", err)
			}

			text := extractText(string(body))

			// Truncate to avoid token explosion
			if len(text) > 15000 {
				text = text[:15000] + "\n\n... (truncated)"
			}

			return fmt.Sprintf("Status: %d\nURL: %s\n\n%s", resp.StatusCode, params.URL, text), nil
		},
	})
}

// RegisterWebSearchTool registers the web_search tool using Brave Search API.
func RegisterWebSearchTool(r *Registry, apiKey string) {
	if apiKey == "" {
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}

	r.Register(&Tool{
		Name:        "web_search",
		Description: "Search the web using Brave Search. Returns titles, URLs, and snippets. Use this to find information before fetching specific pages.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"query": {"type": "string", "description": "Search query"},
				"count": {"type": "integer", "description": "Number of results (default: 5, max: 20)"}
			},
			"required": ["query"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Query string `json:"query"`
				Count int    `json:"count"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}
			if params.Count <= 0 {
				params.Count = 5
			}
			if params.Count > 20 {
				params.Count = 20
			}

			reqURL := fmt.Sprintf("https://api.search.brave.com/res/v1/web/search?q=%s&count=%d",
				url.QueryEscape(params.Query), params.Count)

			req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
			if err != nil {
				return "", fmt.Errorf("web_search: %w", err)
			}
			req.Header.Set("Accept", "application/json")
			req.Header.Set("X-Subscription-Token", apiKey)

			resp, err := client.Do(req)
			if err != nil {
				return "", fmt.Errorf("web_search: %w", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return "", fmt.Errorf("web_search: read body: %w", err)
			}

			if resp.StatusCode != http.StatusOK {
				return fmt.Sprintf("Search failed (status %d): %s", resp.StatusCode, string(body)), nil
			}

			return formatBraveResults(body), nil
		},
	})
}

// formatBraveResults extracts relevant info from Brave Search API response.
func formatBraveResults(body []byte) string {
	var result struct {
		Web struct {
			Results []struct {
				Title       string `json:"title"`
				URL         string `json:"url"`
				Description string `json:"description"`
			} `json:"results"`
		} `json:"web"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Sprintf("Failed to parse search results: %v", err)
	}

	if len(result.Web.Results) == 0 {
		return "No results found."
	}

	var sb strings.Builder
	for i, r := range result.Web.Results {
		sb.WriteString(fmt.Sprintf("%d. **%s**\n   %s\n   %s\n\n", i+1, r.Title, r.URL, r.Description))
	}
	return sb.String()
}

// extractText strips HTML tags, scripts, styles, and extra whitespace from HTML content.
func extractText(html string) string {
	// Remove script and style blocks
	reScript := regexp.MustCompile(`(?is)<script[^>]*>.*?</script>`)
	html = reScript.ReplaceAllString(html, "")
	reStyle := regexp.MustCompile(`(?is)<style[^>]*>.*?</style>`)
	html = reStyle.ReplaceAllString(html, "")

	// Remove HTML comments
	reComment := regexp.MustCompile(`(?s)<!--.*?-->`)
	html = reComment.ReplaceAllString(html, "")

	// Replace block-level tags with newlines
	reBlock := regexp.MustCompile(`(?i)</(p|div|h[1-6]|li|tr|br|hr)[^>]*>`)
	html = reBlock.ReplaceAllString(html, "\n")
	reBR := regexp.MustCompile(`(?i)<br[^>]*/?>`)
	html = reBR.ReplaceAllString(html, "\n")

	// Remove all remaining tags
	reTag := regexp.MustCompile(`<[^>]+>`)
	html = reTag.ReplaceAllString(html, "")

	// Decode common HTML entities
	html = strings.ReplaceAll(html, "&amp;", "&")
	html = strings.ReplaceAll(html, "&lt;", "<")
	html = strings.ReplaceAll(html, "&gt;", ">")
	html = strings.ReplaceAll(html, "&quot;", "\"")
	html = strings.ReplaceAll(html, "&#39;", "'")
	html = strings.ReplaceAll(html, "&nbsp;", " ")

	// Collapse whitespace
	reSpaces := regexp.MustCompile(`[ \t]+`)
	html = reSpaces.ReplaceAllString(html, " ")

	// Collapse multiple newlines
	reNewlines := regexp.MustCompile(`\n{3,}`)
	html = reNewlines.ReplaceAllString(html, "\n\n")

	return strings.TrimSpace(html)
}
