package domain

type RepoStatus struct {
	Path        string
	IsBare      bool
	HasHead     bool
	HeadHash    string
	HasManifest bool
	Manifest    Manifest
}
