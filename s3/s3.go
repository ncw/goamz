//
// goamz - Go packages to interact with the Amazon Web Services.
//
//   https://wiki.ubuntu.com/goamz
//
// Copyright (c) 2011 Canonical Ltd.
//
// Written by Gustavo Niemeyer <gustavo.niemeyer@canonical.com>
//

package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ncw/goamz/aws"
)

const debug = false

// The S3 type encapsulates operations with an S3 region.
type S3 struct {
	aws.Auth
	aws.Region
	private byte // Reserve the right of using private data.
}

// The Bucket type encapsulates operations with an S3 bucket.
type Bucket struct {
	*S3
	Name string
	// The following fields are only set on buckets returned from the ListBuckets call
	Owner        Owner     // Owner of the bucket
	CreationDate time.Time // When the bucket was created
}

// The Owner type represents the owner of the object in an S3 bucket.
type Owner struct {
	ID          string
	DisplayName string
}

var attempts = aws.AttemptStrategy{
	Min:   5,
	Total: 5 * time.Second,
	Delay: 200 * time.Millisecond,
}

// New creates a new S3.
func New(auth aws.Auth, region aws.Region) *S3 {
	return &S3{auth, region, 0}
}

// Bucket returns a Bucket with the given name.
func (s3 *S3) Bucket(name string) *Bucket {
	if s3.Region.S3BucketEndpoint != "" || s3.Region.S3LowercaseBucket {
		name = strings.ToLower(name)
	}
	return &Bucket{S3: s3, Name: name}
}

// bucketsResp is returned by S3.Buckets
type bucketsResp struct {
	Owner   Owner
	Buckets []struct {
		Name         string
		CreationDate string // Date the bucket was created, e.g., 2009-02-03T16:45:09.000Z
	} `xml:"Buckets>Bucket"`
}

// List returns a list of all buckets owned by the sender
//
// See http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTServiceGET.html
func (s3 *S3) ListBuckets() (buckets []*Bucket, err error) {
	req := &request{
		method: "GET",
		bucket: "",
		path:   "",
	}
	result := &bucketsResp{}
	err = s3.query(req, result)
	if err != nil {
		return nil, err
	}
	buckets = make([]*Bucket, len(result.Buckets))
	for i := range buckets {
		info := result.Buckets[i]
		b := s3.Bucket(info.Name)
		b.CreationDate, err = time.Parse(time.RFC3339Nano, info.CreationDate)
		if err != nil {
			return nil, err
		}
		b.Owner = result.Owner
		buckets[i] = b
	}
	return buckets, nil
}

var createBucketConfiguration = `<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"> 
  <LocationConstraint>%s</LocationConstraint> 
</CreateBucketConfiguration>`

// locationConstraint returns an io.Reader specifying a LocationConstraint if
// required for the region.
//
// See http://goo.gl/bh9Kq for details.
func (s3 *S3) locationConstraint() io.Reader {
	constraint := ""
	if s3.Region.S3LocationConstraint {
		constraint = fmt.Sprintf(createBucketConfiguration, s3.Region.Name)
	}
	return strings.NewReader(constraint)
}

type ACL string

const (
	Private           = ACL("private")
	PublicRead        = ACL("public-read")
	PublicReadWrite   = ACL("public-read-write")
	AuthenticatedRead = ACL("authenticated-read")
	BucketOwnerRead   = ACL("bucket-owner-read")
	BucketOwnerFull   = ACL("bucket-owner-full-control")
)

// Headers stores HTTP headers (can only have one of each header which
// simplifies the use).
type Headers map[string]string

// readHeaders returns a Headers object from an http.Response.
func readHeaders(resp *http.Response) Headers {
	headers := Headers{}
	for key, values := range resp.Header {
		headers[key] = values[0]
	}
	return headers
}

// writeHeaders adds headers (if any) to the *http.Header passed in
func (h Headers) writeHeaders(out *http.Header) {
	if h == nil {
		return
	}
	if *out == nil {
		*out = make(http.Header)
	}
	for key, value := range h {
		out.Add(key, value)
	}
}

// PutBucket creates a new bucket.
//
// See http://goo.gl/ndjnR for details.
func (b *Bucket) PutBucket(perm ACL) error {
	headers := map[string][]string{
		"x-amz-acl": {string(perm)},
	}
	req := &request{
		method:  "PUT",
		bucket:  b.Name,
		path:    "/",
		headers: headers,
		payload: b.locationConstraint(),
	}
	return b.S3.query(req, nil)
}

// DelBucket removes an existing S3 bucket. All objects in the bucket must
// be removed before the bucket itself can be removed.
//
// See http://goo.gl/GoBrY for details.
func (b *Bucket) DelBucket() (err error) {
	req := &request{
		method: "DELETE",
		bucket: b.Name,
		path:   "/",
	}
	for attempt := attempts.Start(); attempt.Next(); {
		err = b.S3.query(req, nil)
		if !shouldRetry(err) {
			break
		}
	}
	return err
}

// Get retrieves an object from an S3 bucket.
//
// See http://goo.gl/isCO7 for details.
func (b *Bucket) Get(path string) (data []byte, err error) {
	body, err := b.GetReader(path)
	if err != nil {
		return nil, err
	}
	data, err = ioutil.ReadAll(body)
	body.Close()
	return data, err
}

// GetReaderHeaders retrieves an object from an S3 bucket.  It is the
// caller's responsibility to call Close on the io.ReadCloser when
// finished reading.  Headers are added to the request and returned
// from the request.
func (b *Bucket) GetReaderHeaders(path string, headers Headers) (io.ReadCloser, Headers, error) {
	req := &request{
		bucket: b.Name,
		path:   path,
	}
	headers.writeHeaders(&req.headers)
	err := b.S3.prepare(req)
	if err != nil {
		return nil, nil, err
	}
	for attempt := attempts.Start(); attempt.Next(); {
		hresp, err := b.S3.run(req)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		return hresp.Body, readHeaders(hresp), nil
	}
	panic("unreachable")
}

// GetReader retrieves an object from an S3 bucket.
// It is the caller's responsibility to call Close on rc when
// finished reading.
func (b *Bucket) GetReader(path string) (io.ReadCloser, error) {
	rc, _, error := b.GetReaderHeaders(path, nil)
	return rc, error
}

// Put inserts an object into the S3 bucket.
//
// See http://goo.gl/FEBPD for details.
func (b *Bucket) Put(path string, data []byte, contType string, perm ACL) error {
	body := bytes.NewBuffer(data)
	return b.PutReader(path, body, int64(len(data)), contType, perm)
}

// PutReaderHeaders inserts an object into the S3 bucket by consuming
// data from r until EOF. Headers are added to the request and
// returned from the request.
func (b *Bucket) PutReaderHeaders(path string, r io.Reader, length int64, contType string, perm ACL, headers Headers) (Headers, error) {
	h := map[string][]string{
		"Content-Length": {strconv.FormatInt(length, 10)},
		"Content-Type":   {contType},
		"x-amz-acl":      {string(perm)},
	}
	req := &request{
		method:  "PUT",
		bucket:  b.Name,
		path:    path,
		headers: h,
		payload: r,
	}
	return b.S3.queryHeaders(req, headers, nil)
}

// PutReader inserts an object into the S3 bucket by consuming data
// from r until EOF.
func (b *Bucket) PutReader(path string, r io.Reader, length int64, contType string, perm ACL) error {
	_, err := b.PutReaderHeaders(path, r, length, contType, perm, nil)
	return err
}

// CopyObjectResult is returned as the result of an object copy
type CopyObjectResult struct {
	LastModified string
	// ETag gives the hex-encoded MD5 sum of the contents,
	// surrounded with double-quotes.
	ETag string
}

// Copy creates this object by copying from another object in srcBucket/srcPath
//
// If copyMetadata is true the metadata is copied from the srcPath
// object, otherwise it is written from headers
//
// Note that you'll need to set the Content-Type in headers if copyMetadata is false
//
// The ACL must be set
//
// Headers are added and returned to and from the request.
func (b *Bucket) Copy(dstPath, srcBucket, srcPath string, copyMetadata bool, perm ACL, headers Headers) (Headers, *CopyObjectResult, error) {
	copyOrReplace := "REPLACE"
	if copyMetadata {
		copyOrReplace = "COPY"
	}
	h := map[string][]string{
		"Content-Length":           {"0"},
		"x-amz-acl":                {string(perm)},
		"x-amz-copy-source":        {srcBucket + "/" + srcPath},
		"x-amz-metadata-directive": {copyOrReplace},
	}

	req := &request{
		method:  "PUT",
		bucket:  b.Name,
		path:    dstPath,
		headers: h,
	}
	result := &CopyObjectResult{}
	resultHeaders, err := b.S3.queryHeaders(req, headers, result)
	return resultHeaders, result, err
}

// Updates the metadata for this object.  All the metadata is replaced
// with that passed in.  This works by calling Copy on the same object.
//
// Note that you'll need to set the Content-Type in headers
//
// The ACL must be set also
//
// headers are added and returned
func (b *Bucket) Update(path string, perm ACL, headers Headers) (Headers, error) {
	headers, _, err := b.Copy(path, b.Name, path, false, perm, headers)
	return headers, err
}

// Head returns headers from an S3 object
//
// Any Headers passed in are added to the request
//
// http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectHEAD.html
func (b *Bucket) Head(path string, headers Headers) (Headers, error) {
	req := &request{
		method: "HEAD",
		bucket: b.Name,
		path:   path,
	}
	return b.S3.queryHeaders(req, headers, nil)
}

// Del removes an object from the S3 bucket.
//
// See http://goo.gl/APeTt for details.
func (b *Bucket) Del(path string) error {
	req := &request{
		method: "DELETE",
		bucket: b.Name,
		path:   path,
	}
	return b.S3.query(req, nil)
}

// The ListResp type holds the results of a List bucket operation.
type ListResp struct {
	Name       string
	Prefix     string
	Delimiter  string
	Marker     string
	NextMarker string
	MaxKeys    int
	// IsTruncated is true if the results have been truncated because
	// there are more keys and prefixes than can fit in MaxKeys.
	// N.B. this is the opposite sense to that documented (incorrectly) in
	// http://goo.gl/YjQTc
	IsTruncated    bool
	Contents       []Key
	CommonPrefixes []string `xml:">Prefix"`
}

// The Key type represents an item stored in an S3 bucket.
type Key struct {
	Key          string
	LastModified string
	Size         int64
	// ETag gives the hex-encoded MD5 sum of the contents,
	// surrounded with double-quotes.
	ETag         string
	StorageClass string
	Owner        Owner
}

// List returns information about objects in an S3 bucket.
//
// The prefix parameter limits the response to keys that begin with the
// specified prefix.
//
// The delim parameter causes the response to group all of the keys that
// share a common prefix up to the next delimiter in a single entry within
// the CommonPrefixes field. You can use delimiters to separate a bucket
// into different groupings of keys, similar to how folders would work.
//
// The marker parameter specifies the key to start with when listing objects
// in a bucket. Amazon S3 lists objects in alphabetical order and
// will return keys alphabetically greater than the marker.
//
// The max parameter specifies how many keys + common prefixes to return in
// the response. The default is 1000.
//
// For example, given these keys in a bucket:
//
//     index.html
//     index2.html
//     photos/2006/January/sample.jpg
//     photos/2006/February/sample2.jpg
//     photos/2006/February/sample3.jpg
//     photos/2006/February/sample4.jpg
//
// Listing this bucket with delimiter set to "/" would yield the
// following result:
//
//     &ListResp{
//         Name:      "sample-bucket",
//         MaxKeys:   1000,
//         Delimiter: "/",
//         Contents:  []Key{
//             {Key: "index.html", "index2.html"},
//         },
//         CommonPrefixes: []string{
//             "photos/",
//         },
//     }
//
// Listing the same bucket with delimiter set to "/" and prefix set to
// "photos/2006/" would yield the following result:
//
//     &ListResp{
//         Name:      "sample-bucket",
//         MaxKeys:   1000,
//         Delimiter: "/",
//         Prefix:    "photos/2006/",
//         CommonPrefixes: []string{
//             "photos/2006/February/",
//             "photos/2006/January/",
//         },
//     }
//
// See http://goo.gl/YjQTc for details.
func (b *Bucket) List(prefix, delim, marker string, max int) (result *ListResp, err error) {
	params := map[string][]string{
		"prefix":    {prefix},
		"delimiter": {delim},
		"marker":    {marker},
	}
	if max != 0 {
		params["max-keys"] = []string{strconv.FormatInt(int64(max), 10)}
	}
	req := &request{
		bucket: b.Name,
		params: params,
	}
	result = &ListResp{}
	for attempt := attempts.Start(); attempt.Next(); {
		err = b.S3.query(req, result)
		if !shouldRetry(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// URL returns a non-signed URL that allows retriving the
// object at path. It only works if the object is publicly
// readable (see SignedURL).
func (b *Bucket) URL(path string) string {
	req := &request{
		bucket: b.Name,
		path:   path,
	}
	err := b.S3.prepare(req)
	if err != nil {
		panic(err)
	}
	u, err := req.url()
	if err != nil {
		panic(err)
	}
	u.RawQuery = ""
	return u.String()
}

// SignedURL returns a signed URL that allows anyone holding the URL
// to retrieve the object at path. The signature is valid until expires.
func (b *Bucket) SignedURL(path string, expires time.Time) string {
	req := &request{
		bucket: b.Name,
		path:   path,
		params: url.Values{"Expires": {strconv.FormatInt(expires.Unix(), 10)}},
	}
	err := b.S3.prepare(req)
	if err != nil {
		panic(err)
	}
	u, err := req.url()
	if err != nil {
		panic(err)
	}
	return u.String()
}

type request struct {
	method   string
	bucket   string
	path     string
	signpath string
	params   url.Values
	headers  http.Header
	baseurl  string
	payload  io.Reader
	prepared bool
}

func (req *request) url() (*url.URL, error) {
	u, err := url.Parse(req.baseurl)
	if err != nil {
		return nil, fmt.Errorf("bad S3 endpoint URL %q: %v", req.baseurl, err)
	}
	u.RawQuery = req.params.Encode()
	u.Path = req.path
	return u, nil
}

// query prepares and runs the req request.
// If resp is not nil, the XML data contained in the response
// body will be unmarshalled on it.
func (s3 *S3) query(req *request, resp interface{}) error {
	err := s3.prepare(req)
	if err != nil {
		return err
	}
	hresp, err := s3.run(req)
	if err != nil {
		return err
	}
	if resp != nil {
		err = xml.NewDecoder(hresp.Body).Decode(resp)
	}
	hresp.Body.Close()
	return nil
}

// queryHeaders prepares and runs the req request.
// If headers is not nil the Headers will be added
// If resp is not nil, the XML data contained in the response
// body will be unmarshalled on it.
// Any headers in the response will be returned
func (s3 *S3) queryHeaders(req *request, headers Headers, resp interface{}) (Headers, error) {
	headers.writeHeaders(&req.headers)
	err := s3.prepare(req)
	if err != nil {
		return nil, err
	}
	hresp, err := s3.run(req)
	if err != nil {
		return nil, err
	}
	return readHeaders(hresp), nil
}

// prepare sets up req to be delivered to S3.
func (s3 *S3) prepare(req *request) error {
	if !req.prepared {
		req.prepared = true
		if req.method == "" {
			req.method = "GET"
		}
		// Copy so they can be mutated without affecting on retries.
		params := make(url.Values)
		headers := make(http.Header)
		for k, v := range req.params {
			params[k] = v
		}
		for k, v := range req.headers {
			headers[k] = v
		}
		req.params = params
		req.headers = headers
		if !strings.HasPrefix(req.path, "/") {
			req.path = "/" + req.path
		}
		req.signpath = req.path
		req.baseurl = s3.Region.S3BucketEndpoint
		if req.baseurl == "" {
			// Use the path method to address the bucket.
			req.baseurl = s3.Region.S3Endpoint
			if req.bucket != "" {
				req.path = "/" + req.bucket + req.path
			}
		} else {
			// Just in case, prevent injection.
			if strings.IndexAny(req.bucket, "/:@") >= 0 {
				return fmt.Errorf("bad S3 bucket: %q", req.bucket)
			}
			req.baseurl = strings.Replace(req.baseurl, "${bucket}", req.bucket, -1)
		}
		if req.bucket != "" {
			req.signpath = "/" + req.bucket + req.signpath
		}
	}

	// Always sign again as it's not clear how far the
	// server has handled a previous attempt.
	u, err := url.Parse(req.baseurl)
	if err != nil {
		return fmt.Errorf("bad S3 endpoint URL %q: %v", req.baseurl, err)
	}
	req.signpath = (&url.URL{Path: req.signpath}).String()
	req.headers["Host"] = []string{u.Host}
	req.headers["Date"] = []string{time.Now().In(time.UTC).Format(time.RFC1123)}
	sign(s3.Auth, req.method, req.signpath, req.params, req.headers)
	return nil
}

// run sends req and returns the http response from the server.
func (s3 *S3) run(req *request) (*http.Response, error) {
	if debug {
		log.Printf("Running S3 request: %#v", req)
	}

	u, err := req.url()
	if err != nil {
		return nil, err
	}

	hreq := http.Request{
		URL:        u,
		Method:     req.method,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Close:      true,
		Header:     req.headers,
	}

	if v, ok := req.headers["Content-Length"]; ok {
		hreq.ContentLength, _ = strconv.ParseInt(v[0], 10, 64)
		delete(req.headers, "Content-Length")
	}
	if req.payload != nil {
		hreq.Body = ioutil.NopCloser(req.payload)
	}

	hresp, err := http.DefaultClient.Do(&hreq)
	if err != nil {
		return nil, err
	}
	if debug {
		dump, _ := httputil.DumpResponse(hresp, true)
		log.Printf("} -> %s\n", dump)
	}
	if hresp.StatusCode != 200 && hresp.StatusCode != 204 {
		return nil, buildError(hresp)
	}
	return hresp, err
}

// Error represents an error in an operation with S3.
type Error struct {
	StatusCode int    // HTTP status code (200, 403, ...)
	Code       string // EC2 error code ("UnsupportedOperation", ...)
	Message    string // The human-oriented error message
	BucketName string
	RequestId  string
	HostId     string
}

func (e *Error) Error() string {
	return e.Message
}

func buildError(r *http.Response) error {
	if debug {
		log.Printf("got error (status code %v)", r.StatusCode)
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("\tread error: %v", err)
		} else {
			log.Printf("\tdata:\n%s\n\n", data)
		}
		r.Body = ioutil.NopCloser(bytes.NewBuffer(data))
	}

	err := Error{}
	// TODO return error if Unmarshal fails?
	xml.NewDecoder(r.Body).Decode(&err)
	r.Body.Close()
	err.StatusCode = r.StatusCode
	if err.Message == "" {
		err.Message = r.Status
	}
	if debug {
		log.Printf("err: %#v\n", err)
	}
	return &err
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	switch err {
	case io.ErrUnexpectedEOF, io.EOF:
		return true
	}
	switch e := err.(type) {
	case *net.DNSError:
		return true
	case *net.OpError:
		switch e.Op {
		case "read", "write":
			return true
		}
	case *Error:
		switch e.Code {
		case "InternalError", "NoSuchUpload", "NoSuchBucket":
			return true
		}
	}
	return false
}

func hasCode(err error, code string) bool {
	s3err, ok := err.(*Error)
	return ok && s3err.Code == code
}
