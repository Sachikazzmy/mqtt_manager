package cli

import (
	"os"
	"testing"
)

func TestReadSecretLine(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, err := writer.Write([]byte("secret-value\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	value, err := readSecretLine(reader)
	if err != nil {
		t.Fatal(err)
	}
	if value != "secret-value" {
		t.Fatalf("读取的密钥 = %q", value)
	}
}

func TestReadSecretLineEOF(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, err := writer.Write([]byte("secret-without-newline")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := readSecretLine(reader); err == nil {
		t.Fatal("EOF 应该返回错误")
	}
}
