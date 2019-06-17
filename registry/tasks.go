package registry

import (
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/hhkbp2/go-logging"
	"github.com/tidwall/gjson"
)

/*PurgeConfig represent the configuration for tag purge */
type PurgeConfig struct {
	RepoRegex     string `yaml:"repo_regex"`
	TagsRegex     string `yaml:"tags_regex"`
	TagsKeepDays  int    `yaml:"tags_keep_days"`
	TagsKeepCount int    `yaml:"tags_keep_count"`
}

type tagData struct {
	name    string
	created time.Time
}

func (t tagData) String() string {
	return fmt.Sprintf(`"%s <%s>"`, t.name, t.created.Format("2006-01-02 15:04:05"))
}

type timeSlice []tagData

func (p timeSlice) Len() int {
	return len(p)
}

func (p timeSlice) Less(i, j int) bool {
	return p[i].created.After(p[j].created)
}

func (p timeSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// PurgeOldTags purge old tags.
func PurgeOldTags(client *Client, purgeDryRun bool, purgeTagsKeepDays, purgeTagsKeepCount int, purgeTagsConfig []PurgeConfig) {
	logger := SetupLogging("registry.tasks.PurgeOldTags")
	// Reduce client logging.
	//client.logger.SetLevel(logging.LevelError)

	// Add the global configuration at the end of purgeTagsConfig to use it when no other rule match
	purgeTagsConfig = append(purgeTagsConfig, PurgeConfig{
		RepoRegex:     ".*",
		TagsRegex:     ".*",
		TagsKeepDays:  purgeTagsKeepDays,
		TagsKeepCount: purgeTagsKeepCount,
	})

	dryRunText := ""
	if purgeDryRun {
		logger.Warn("Dry-run mode enabled.")
		dryRunText = "skipped"
	}

	logger.Info("Scanning registry for repositories, tags and their creation dates...")
	catalog := client.Repositories(true)
	// catalog := map[string][]string{"library": []string{""}}
	now := time.Now().UTC()

	for namespace := range catalog {
		for _, repo := range catalog[namespace] {
			analyzeRepo(client, namespace, repo, purgeTagsConfig, now, purgeDryRun, dryRunText, logger)
		}
	}

	logger.Info("Done.")
}

func analyzeRepo(client *Client, namespace string, repo string, purgeTagsConfig []PurgeConfig, now time.Time, purgeDryRun bool, dryRunText string, logger logging.Logger) {
	tagsFromRepo := timeSlice{}
	purgeTags := []string{}
	keepTags := []string{}
	count := 0
	var purgeConfig *PurgeConfig

	if namespace != "library" {
		repo = fmt.Sprintf("%s/%s", namespace, repo)
	}

	logger.Infof("[%s] Processing repo %s", repo, repo)

	for _, config := range purgeTagsConfig {
		logger.Infof("[%s] Repo regex: %s", repo, config.RepoRegex)
		re, err := regexp.Compile(config.RepoRegex)
		if err != nil {
			logger.Warnf("[%s] Skipping repo because regex don't compile: %s", repo, err)
			return
		}
		matchIndexes := re.FindStringIndex(repo)
		if matchIndexes != nil {
			purgeConfig = &config
			break
		}
	}

	if purgeConfig == nil {
		logger.Infof("[%s] No match found for repo, skipping it", repo)
		return
	}

	tags := client.Tags(repo)
	logger.Infof("[%s] scanning %d tags...", repo, len(tags))
	if len(tags) == 0 {
		return
	}

	for _, tag := range tags {

		logger.Infof("[%s] Checking if tag '%s' match the tag regex: %s", repo, tag, purgeConfig.TagsRegex)
		re, err := regexp.Compile(purgeConfig.TagsRegex)
		if err != nil {
			logger.Warnf("[%s] Skipping tag %s because regex don't compile: %s", repo, tag, err)
			return
		}
		matchIndexes := re.FindStringIndex(tag)
		if matchIndexes == nil {
			logger.Infof("[%s] Skipping tag %s because it doesn't match the regex %s", repo, tag, purgeConfig.TagsRegex)
			continue
		}

		_, infoV1, _ := client.TagInfo(repo, tag, true)
		if infoV1 == "" {
			logger.Errorf("[%s] missing manifest v1 for tag %s", repo, tag)
			continue
		}
		created := gjson.Get(gjson.Get(infoV1, "history.0.v1Compatibility").String(), "created").Time()
		tagsFromRepo = append(tagsFromRepo, tagData{name: tag, created: created})
	}

	// Sort tags by "created" from newest to oldest.
	sortedTags := make(timeSlice, 0, len(tagsFromRepo))
	for _, d := range tagsFromRepo {
		sortedTags = append(sortedTags, d)
	}
	sort.Sort(sortedTags)
	tagsFromRepo = sortedTags

	// Filter out tags by retention days.
	for _, tag := range tagsFromRepo {
		delta := int(now.Sub(tag.created).Hours() / 24)
		if delta > purgeConfig.TagsKeepDays {
			purgeTags = append(purgeTags, tag.name)
		} else {
			keepTags = append(keepTags, tag.name)
		}
	}

	// Keep minimal count of tags no matter how old they are.
	if len(tagsFromRepo)-len(purgeTags) < purgeConfig.TagsKeepCount {
		if len(purgeTags) > purgeConfig.TagsKeepCount {
			keepTags = append(keepTags, purgeTags[:purgeConfig.TagsKeepCount]...)
			purgeTags = purgeTags[purgeConfig.TagsKeepCount:]
		} else {
			keepTags = append(keepTags, purgeTags...)
			purgeTags = []string{}
		}
	}

	count = count + len(purgeTags)
	logger.Infof("[%s] All %d: %v", repo, len(tagsFromRepo), tagsFromRepo)
	logger.Infof("[%s] Keep %d: %v", repo, len(keepTags), keepTags)
	logger.Infof("[%s] Purge %d: %v", repo, len(purgeTags), purgeTags)

	logger.Infof("There are %d tags to purge.", count)
	if count > 0 {
		logger.Info("Purging old tags...")
	}

	for _, repo := range purgeTags {
		logger.Infof("[%s] Purging %d tags... %s", repo, len(purgeTags), dryRunText)
		if purgeDryRun {
			continue
		}
		for _, tag := range purgeTags {
			client.DeleteTag(repo, tag)
		}
	}
}
