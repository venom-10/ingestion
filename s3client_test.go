package main

import (
	"os"
	"testing"
)

func TestResolveRegion(t *testing.T) {
	// Save original env
	origRegion := os.Getenv("AWS_REGION")
	origLegacyRegion := os.Getenv("REGION")
	origEndpoint := os.Getenv("S3_ENDPOINT")
	defer func() {
		os.Setenv("AWS_REGION", origRegion)
		os.Setenv("REGION", origLegacyRegion)
		os.Setenv("S3_ENDPOINT", origEndpoint)
	}()

	tests := []struct {
		name          string
		awsRegion     string
		legacyRegion  string
		s3Endpoint    string
		expectedRegion string
	}{
		{
			name:           "AWS_REGION set",
			awsRegion:      "eu-west-1",
			legacyRegion:   "",
			s3Endpoint:     "",
			expectedRegion: "eu-west-1",
		},
		{
			name:           "REGION (legacy) set",
			awsRegion:      "",
			legacyRegion:   "ap-southeast-2",
			s3Endpoint:     "",
			expectedRegion: "ap-southeast-2",
		},
		{
			name:           "AWS_REGION takes precedence over REGION",
			awsRegion:      "us-west-2",
			legacyRegion:   "eu-central-1",
			s3Endpoint:     "",
			expectedRegion: "us-west-2",
		},
		{
			name:           "defaults to us-east-1 when no endpoint",
			awsRegion:      "",
			legacyRegion:   "",
			s3Endpoint:     "",
			expectedRegion: "us-east-1",
		},
		{
			name:           "no default when custom endpoint set",
			awsRegion:      "",
			legacyRegion:   "",
			s3Endpoint:     "http://localhost:9000",
			expectedRegion: "",
		},
		{
			name:           "explicit region with custom endpoint",
			awsRegion:      "eu-west-1",
			legacyRegion:   "",
			s3Endpoint:     "http://localhost:9000",
			expectedRegion: "eu-west-1",
		},
		{
			name:           "legacy region with custom endpoint",
			awsRegion:      "",
			legacyRegion:   "ap-northeast-1",
			s3Endpoint:     "http://rustfs:9000",
			expectedRegion: "ap-northeast-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("AWS_REGION", tt.awsRegion)
			os.Setenv("REGION", tt.legacyRegion)
			os.Setenv("S3_ENDPOINT", tt.s3Endpoint)

			region := resolveRegion()
			if region != tt.expectedRegion {
				t.Errorf("resolveRegion() = %q, want %q", region, tt.expectedRegion)
			}
		})
	}
}