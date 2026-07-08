package handlers

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
)

// Dataset preview + schema explorer + per-column stats. Powers the dataset
// detail page (schema table, column-quality heatmap, histograms).
//
// Implementation: ranged S3 read of the first previewReadBytes of the file,
// parsed for csv/tsv/jsonl. Stats are computed over up to statsSampleRows
// parsed rows and flagged as sampled when the file is larger. Columnar
// formats (parquet/hdf5/arrow) need the ML tier and return 422 for now.

const (
	previewReadBytes = int64(2 << 20) // 2 MiB
	previewRows      = 50
	statsSampleRows  = 5000
	histogramBuckets = 10
	topKCategories   = 10
)

var nullTokens = map[string]bool{"": true, "null": true, "NULL": true, "Null": true, "NA": true, "N/A": true, "na": true, "NaN": true, "nan": true, "None": true}

type parsedDataset struct {
	columns  []string
	rows     [][]string // row-major, aligned with columns
	complete bool       // whole file parsed (not truncated)
}

// loadDatasetSample fetches and parses the head of the dataset file.
func loadDatasetSample(ctx context.Context, dataset *models.Dataset) (*parsedDataset, error) {
	if privacyStorage == nil {
		return nil, fmt.Errorf("storage not configured")
	}
	name := strings.ToLower(dataset.Filename)
	format := ""
	switch {
	case strings.HasSuffix(name, ".csv"):
		format = "csv"
	case strings.HasSuffix(name, ".tsv"):
		format = "tsv"
	case strings.HasSuffix(name, ".jsonl") || strings.HasSuffix(name, ".ndjson"):
		format = "jsonl"
	default:
		return nil, fmt.Errorf("unsupported")
	}

	raw, complete, err := privacyStorage.GetObjectRange(ctx, dataset.S3Path, previewReadBytes)
	if err != nil {
		return nil, err
	}
	// When truncated, drop the final (possibly partial) line.
	if !complete {
		if idx := strings.LastIndexByte(string(raw), '\n'); idx > 0 {
			raw = raw[:idx]
		}
	}

	out := &parsedDataset{complete: complete}
	switch format {
	case "csv", "tsv":
		r := csv.NewReader(strings.NewReader(string(raw)))
		if format == "tsv" {
			r.Comma = '\t'
		}
		r.LazyQuotes = true
		r.FieldsPerRecord = -1
		records, rerr := r.ReadAll()
		if rerr != nil || len(records) == 0 {
			return nil, fmt.Errorf("could not parse file: %v", rerr)
		}
		out.columns = records[0]
		for _, rec := range records[1:] {
			if len(out.rows) >= statsSampleRows {
				out.complete = false
				break
			}
			// Pad/trim ragged rows to the header width.
			row := make([]string, len(out.columns))
			for i := range row {
				if i < len(rec) {
					row[i] = rec[i]
				}
			}
			out.rows = append(out.rows, row)
		}
	case "jsonl":
		lines := strings.Split(string(raw), "\n")
		colSet := []string{}
		seen := map[string]bool{}
		var objs []map[string]interface{}
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if len(objs) >= statsSampleRows {
				out.complete = false
				break
			}
			var obj map[string]interface{}
			if json.Unmarshal([]byte(line), &obj) != nil {
				continue
			}
			objs = append(objs, obj)
			for k := range obj {
				if !seen[k] {
					seen[k] = true
					colSet = append(colSet, k)
				}
			}
		}
		if len(objs) == 0 {
			return nil, fmt.Errorf("could not parse file: no valid JSON lines")
		}
		sort.Strings(colSet)
		out.columns = colSet
		for _, obj := range objs {
			row := make([]string, len(colSet))
			for i, k := range colSet {
				if v, ok := obj[k]; ok && v != nil {
					row[i] = fmt.Sprintf("%v", v)
				}
			}
			out.rows = append(out.rows, row)
		}
	}
	return out, nil
}

// inferColumnType classifies sampled values: integer, float, boolean, datetime, string.
func inferColumnType(values []string) string {
	ints, floats, bools, dates, total := 0, 0, 0, 0, 0
	for _, v := range values {
		if nullTokens[v] {
			continue
		}
		total++
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			ints++
			continue
		}
		if _, err := strconv.ParseFloat(v, 64); err == nil {
			floats++
			continue
		}
		lv := strings.ToLower(v)
		if lv == "true" || lv == "false" {
			bools++
			continue
		}
		if parseAnyDate(v) {
			dates++
		}
	}
	if total == 0 {
		return "string"
	}
	switch {
	case ints == total:
		return "integer"
	case ints+floats == total:
		return "float"
	case bools == total:
		return "boolean"
	case dates > total*8/10:
		return "datetime"
	default:
		return "string"
	}
}

func parseAnyDate(v string) bool {
	for _, layout := range []string{time.RFC3339, "2006-01-02", "2006-01-02 15:04:05", "01/02/2006", "2006/01/02"} {
		if _, err := time.Parse(layout, v); err == nil {
			return true
		}
	}
	return false
}

// columnValues extracts one column from row-major data.
func columnValues(p *parsedDataset, idx int) []string {
	vals := make([]string, 0, len(p.rows))
	for _, r := range p.rows {
		vals = append(vals, r[idx])
	}
	return vals
}

func fetchDatasetForInsights(c *fiber.Ctx, ctx context.Context) (*models.Dataset, error) {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)
	dataset, err := repository.NewDatasetRepository(database.GetDB()).GetByID(ctx, datasetID)
	if err != nil {
		return nil, c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{"code": "NOT_FOUND", "message": "Dataset not found"},
		})
	}
	if dataset.UserID != userID {
		return nil, c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{"code": "FORBIDDEN", "message": "You do not have access to this dataset"},
		})
	}
	return dataset, nil
}

// GetDatasetPreviewFiber returns schema + first rows.
// GET /datasets/:id/preview
func GetDatasetPreviewFiber(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dataset, ferr := fetchDatasetForInsights(c, ctx)
	if dataset == nil {
		return ferr
	}

	parsed, err := loadDatasetSample(ctx, dataset)
	if err != nil {
		if err.Error() == "unsupported" {
			return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
				"error": fiber.Map{"code": "UNSUPPORTED_FORMAT", "message": "Preview supports csv, tsv and jsonl files for now"},
			})
		}
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error": fiber.Map{"code": "PREVIEW_FAILED", "message": "Could not read the dataset file"},
		})
	}

	columns := make([]fiber.Map, 0, len(parsed.columns))
	for i, name := range parsed.columns {
		vals := columnValues(parsed, i)
		nulls := 0
		uniques := map[string]bool{}
		samples := []string{}
		for _, v := range vals {
			if nullTokens[v] {
				nulls++
				continue
			}
			if !uniques[v] && len(samples) < 5 {
				samples = append(samples, truncateValue(v, 80))
			}
			uniques[v] = true
		}
		n := len(vals)
		nullPct, uniquePct := 0.0, 0.0
		if n > 0 {
			nullPct = math.Round(float64(nulls)/float64(n)*10000) / 100
			uniquePct = math.Round(float64(len(uniques))/float64(n)*10000) / 100
		}
		columns = append(columns, fiber.Map{
			"name":          name,
			"type":          inferColumnType(vals),
			"null_pct":      nullPct,
			"unique_pct":    uniquePct,
			"sample_values": samples,
		})
	}

	previewCount := previewRows
	if len(parsed.rows) < previewCount {
		previewCount = len(parsed.rows)
	}
	rows := make([]map[string]string, 0, previewCount)
	for _, r := range parsed.rows[:previewCount] {
		obj := map[string]string{}
		for i, name := range parsed.columns {
			obj[name] = truncateValue(r[i], 200)
		}
		rows = append(rows, obj)
	}

	rowCount := int64(len(parsed.rows))
	if dataset.RowCount != nil && *dataset.RowCount > 0 {
		rowCount = *dataset.RowCount
	}

	return c.JSON(fiber.Map{
		"dataset_id":   dataset.ID,
		"dataset_name": dataset.Filename,
		"columns":      columns,
		"rows":         rows,
		"row_count":    rowCount,
		"sampled":      !parsed.complete,
		"sampled_rows": len(parsed.rows),
	})
}

// GetDatasetStatsFiber returns per-column distribution stats.
// GET /datasets/:id/stats
func GetDatasetStatsFiber(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dataset, ferr := fetchDatasetForInsights(c, ctx)
	if dataset == nil {
		return ferr
	}

	parsed, err := loadDatasetSample(ctx, dataset)
	if err != nil {
		if err.Error() == "unsupported" {
			return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
				"error": fiber.Map{"code": "UNSUPPORTED_FORMAT", "message": "Stats support csv, tsv and jsonl files for now"},
			})
		}
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error": fiber.Map{"code": "STATS_FAILED", "message": "Could not read the dataset file"},
		})
	}

	columns := make([]fiber.Map, 0, len(parsed.columns))
	for i, name := range parsed.columns {
		vals := columnValues(parsed, i)
		ctype := inferColumnType(vals)
		entry := fiber.Map{"name": name, "type": ctype}

		nulls := 0
		var nums []float64
		counts := map[string]int{}
		for _, v := range vals {
			if nullTokens[v] {
				nulls++
				continue
			}
			if ctype == "integer" || ctype == "float" {
				if f, ferr2 := strconv.ParseFloat(v, 64); ferr2 == nil {
					nums = append(nums, f)
				}
			} else {
				counts[truncateValue(v, 80)]++
			}
		}
		entry["null_count"] = nulls

		if (ctype == "integer" || ctype == "float") && len(nums) > 0 {
			sort.Float64s(nums)
			min, max := nums[0], nums[len(nums)-1]
			sum := 0.0
			for _, f := range nums {
				sum += f
			}
			entry["min"] = min
			entry["max"] = max
			entry["mean"] = math.Round(sum/float64(len(nums))*10000) / 10000
			entry["p50"] = nums[len(nums)/2]

			buckets := []fiber.Map{}
			width := (max - min) / float64(histogramBuckets)
			if width <= 0 { // constant column
				buckets = append(buckets, fiber.Map{"lo": min, "hi": max, "count": len(nums)})
			} else {
				bucketCounts := make([]int, histogramBuckets)
				for _, f := range nums {
					b := int((f - min) / width)
					if b >= histogramBuckets {
						b = histogramBuckets - 1
					}
					bucketCounts[b]++
				}
				for b := 0; b < histogramBuckets; b++ {
					buckets = append(buckets, fiber.Map{
						"lo":    math.Round((min+float64(b)*width)*10000) / 10000,
						"hi":    math.Round((min+float64(b+1)*width)*10000) / 10000,
						"count": bucketCounts[b],
					})
				}
			}
			entry["histogram"] = buckets
		} else {
			type kv struct {
				V string
				N int
			}
			var top []kv
			for v, n := range counts {
				top = append(top, kv{v, n})
			}
			sort.Slice(top, func(a, b int) bool { return top[a].N > top[b].N })
			if len(top) > topKCategories {
				top = top[:topKCategories]
			}
			topOut := []fiber.Map{}
			for _, t := range top {
				topOut = append(topOut, fiber.Map{"value": t.V, "count": t.N})
			}
			entry["top_values"] = topOut
			entry["distinct_count"] = len(counts)
		}
		columns = append(columns, entry)
	}

	return c.JSON(fiber.Map{
		"dataset_id":   dataset.ID,
		"columns":      columns,
		"sampled":      !parsed.complete,
		"sampled_rows": len(parsed.rows),
	})
}

func truncateValue(v string, max int) string {
	if len(v) <= max {
		return v
	}
	return v[:max] + "…"
}
