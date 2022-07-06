package qiniu

// Config for a StorageClient
type QiniuStorageConfig struct {
	Url             string `yaml:"url"`
	AccessKeyId     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	BucketName      string `yaml:"bucket_name"`
	Region          string `yaml:"region"`
	UseHttps        bool   `yaml:"use_https"`
	UseCdn          bool   `yaml:"use_cdn"`
	Private         bool   `yaml:"private"`
}
