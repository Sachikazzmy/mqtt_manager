package main

import "testing"

func TestParseMetricsSupportsPartialSegmentKeys(t *testing.T) {
	metrics, err := parseMetrics("segment-1=0:V,segment-2=101.3:kPa")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 2 || metrics["segment-1"].Value != 0 || metrics["segment-1"].Unit != "V" || metrics["segment-2"].Unit != "kPa" {
		t.Fatalf("解析出的指标子集不正确: %#v", metrics)
	}
	if _, err := parseMetrics("segment-100=1:V"); err != nil {
		t.Fatalf("segment-100 在协议范围内: %v", err)
	}
}

func TestParseMetricsRejectsMissingCustomUnitAndDuplicates(t *testing.T) {
	for _, input := range []string{
		"segment-1=1",
		"temperature=23.6:C",
		"segment-1=1:V,segment-1=2:V",
		"bad key=1:V",
		"voltage=23.6:V",
		"segment-01=23.6:V",
		"segment-101=1:V",
		"segment-1=NaN:V",
	} {
		if _, err := parseMetrics(input); err == nil {
			t.Errorf("parseMetrics(%q) 应返回错误", input)
		}
	}
}
