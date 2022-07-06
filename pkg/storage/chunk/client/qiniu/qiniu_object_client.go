package qiniu

import (
	"bytes"
	"context"
	"fmt"
	"github.com/grafana/loki/pkg/storage/chunk/client"
	"github.com/qiniu/go-sdk/v7/auth"
	"github.com/qiniu/go-sdk/v7/storage"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type QiniuObjectStorage struct {
	mac      *auth.Credentials
	manager  *storage.BucketManager
	uploader *storage.FormUploader
	bucket   string
	base     string
	token    string
	last     int64
	client   *http.Client
	makeUrl  func(*auth.Credentials, string, string) string
}

type listFilesRet struct {
	Marker         string             `json:"marker"`
	Items          []storage.ListItem `json:"items"`
	CommonPrefixes []string           `json:"commonPrefixes"`
}

var _ client.ObjectClient = &QiniuObjectStorage{}

func NewQiniuObjectStorage(cfg *QiniuStorageConfig) (client.ObjectClient, error) {
	q := &QiniuObjectStorage{
		mac:    auth.New(cfg.AccessKeyId, cfg.SecretAccessKey),
		bucket: cfg.BucketName,
		base:   cfg.Url,
		client: &http.Client{Transport: http.DefaultTransport},
		makeUrl: func(_ *auth.Credentials, domain string, key string) string {
			return storage.MakePublicURLv2(domain, key)
		},
	}

	config := &storage.Config{
		Zone:          &storage.ZoneHuadong,
		UseHTTPS:      cfg.UseHttps,
		UseCdnDomains: cfg.UseCdn,
	}
	if "" != cfg.Region {
		if zone, ok := storage.GetRegionByID(storage.RegionID(cfg.Region)); ok {
			config.Zone = &zone
		}
	}
	if cfg.Private {
		q.makeUrl = func(mac *auth.Credentials, domain string, key string) string {
			return storage.MakePrivateURLv2(mac, domain, key, time.Now().Add(time.Hour).Unix())
		}
	}

	q.uploader = storage.NewFormUploaderEx(config, &storage.Client{Client: q.client})
	q.manager = storage.NewBucketManager(q.mac, config)

	return q, nil
}

func (q *QiniuObjectStorage) PutObject(ctx context.Context, objectKey string, object io.ReadSeeker) error {
	if now := time.Now().Unix(); "" == q.token || (now-q.last) > 3600 {
		p := &storage.PutPolicy{Scope: q.bucket, Expires: 4000}
		q.token = p.UploadToken(q.mac)
		q.last = now
	}

	rd := object
	size := int64(0)

	switch r := object.(type) {
	case *bytes.Reader:
		size = r.Size()
	case *strings.Reader:
		size = r.Size()
	default:
		end, _ := object.Seek(0, io.SeekEnd)
		begin, _ := object.Seek(0, io.SeekStart)

		size = end - begin
	}

	return q.uploader.Put(ctx, nil, q.token, objectKey, rd, size, nil)
}

func (q *QiniuObjectStorage) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, int64, error) {
	rawurl := q.makeUrl(q.mac, q.base, objectKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawurl, nil)
	if nil != err {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", "storage")

	resp, err := q.client.Do(req)
	if nil != err {
		return nil, 0, err
	}

	if resp.StatusCode != http.StatusOK {
		err = resp.Body.Close()
		err = fmt.Errorf("http status %d", resp.StatusCode)
		return nil, 0, err
	}

	total, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 63)

	return resp.Body, total, nil
}

func (q *QiniuObjectStorage) List(ctx context.Context, prefix string, delimiter string) (objects []client.StorageObject, prefixes []client.StorageCommonPrefix, err error) {
	host, err := q.manager.RsfReqHost(q.bucket)
	if err != nil {
		return
	}

	for marker, base := "", uriList(host, q.bucket, prefix, delimiter); ; {
		var ret listFilesRet

		rawurl := base + url.QueryEscape(marker)
		if err = q.manager.Client.CredentialedCall(ctx, q.mac, auth.TokenQiniu, &ret, http.MethodPost, rawurl, nil); nil != err {
			break
		}
		for i := range ret.Items {
			objects = append(objects, client.StorageObject{
				Key:        ret.Items[i].Key,
				ModifiedAt: time.Unix(0, ret.Items[i].PutTime*100),
			})
		}
		for _, p := range ret.CommonPrefixes {
			prefixes = append(prefixes, client.StorageCommonPrefix(p))
		}
		if "" == ret.Marker {
			break
		}
		marker = ret.Marker
	}
	return
}

func (q *QiniuObjectStorage) IsObjectNotFoundErr(err error) bool {
	if v := err.Error(); strings.Contains(v, "404") {
		return true
	}
	return false
}

func (q *QiniuObjectStorage) DeleteObject(ctx context.Context, objectKey string) error {
	host, err := q.manager.RsReqHost(q.bucket)
	if err != nil {
		return err
	}

	rawurl := strings.Join([]string{host, storage.URIDelete(q.bucket, objectKey)}, "")

	return q.manager.Client.CredentialedCall(ctx, q.mac, auth.TokenQiniu, nil, http.MethodPost, rawurl, nil)
}

func (q *QiniuObjectStorage) Stop() {

}

func uriList(host, bucket, prefix, delimiter string) string {
	query := make(url.Values)
	query.Add("bucket", bucket)
	if prefix != "" {
		query.Add("prefix", prefix)
	}
	if delimiter != "" {
		query.Add("delimiter", delimiter)
	}
	query.Add("limit", "1000")
	return fmt.Sprintf("%s/list?%s&marker=", host, query.Encode())
}
