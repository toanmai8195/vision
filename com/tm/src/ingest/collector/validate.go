package main

import (
	"errors"
	"fmt"
	"regexp"

	catalogv1 "com.tm/vision/com/tm/proto/vision/catalog/v1"
	eventv1 "com.tm/vision/com/tm/proto/vision/event/v1"
)

// decimalPattern khớp số thập phân có dấu, tối đa 6 chữ số lẻ (DECIMAL(27,6)).
var decimalPattern = regexp.MustCompile(`^-?\d{1,21}(\.\d{1,6})?$`)

// Validate kiểm tra cấu trúc event, chưa đối chiếu catalog.
func Validate(ev *eventv1.DataEvent) error {
	if ev.GetEventId() == "" {
		return errors.New("event_id is required")
	}
	if ev.GetUserId() == "" {
		return errors.New("user_id is required")
	}
	if ev.GetAttr() == "" {
		return errors.New("attr is required")
	}
	if ev.GetEventTsMs() <= 0 {
		return errors.New("event_ts_ms must be positive")
	}

	switch p := ev.GetPayload().(type) {
	case *eventv1.DataEvent_Tag:
		return validateTag(p.Tag)
	case *eventv1.DataEvent_Value:
		return validateDecimal(p.Value.GetValue())
	case *eventv1.DataEvent_TagValue:
		if p.TagValue.GetTag() == "" {
			return errors.New("tag_value.tag is required")
		}
		return validateDecimal(p.TagValue.GetValue())
	case nil:
		return errors.New("payload is required")
	default:
		return fmt.Errorf("unsupported payload %T", p)
	}
}

func validateTag(t *eventv1.TagEvent) error {
	if len(t.GetTagsAdd()) == 0 && len(t.GetTagsRemove()) == 0 {
		return errors.New("tag event needs tags_add or tags_remove")
	}
	added := make(map[string]struct{}, len(t.GetTagsAdd()))
	for _, tag := range t.GetTagsAdd() {
		if tag == "" {
			return errors.New("empty tag in tags_add")
		}
		added[tag] = struct{}{}
	}
	for _, tag := range t.GetTagsRemove() {
		if tag == "" {
			return errors.New("empty tag in tags_remove")
		}
		if _, ok := added[tag]; ok {
			return fmt.Errorf("tag %q is both added and removed in one event", tag)
		}
	}
	return nil
}

func validateDecimal(v string) error {
	if !decimalPattern.MatchString(v) {
		return fmt.Errorf("value %q is not a decimal with at most 6 fractional digits", v)
	}
	return nil
}

// PayloadMatchesDataType kiểm tra payload có đúng kiểu mà attribute khai báo.
// Rẽ nhánh exhaustive theo 4 DataType (CLAUDE.md §11).
func PayloadMatchesDataType(dt catalogv1.DataType, ev *eventv1.DataEvent) error {
	switch dt {
	case catalogv1.DataType_MUTEX:
		t := ev.GetTag()
		if t == nil {
			return errors.New("MUTEX attribute requires a tag payload")
		}
		if len(t.GetTagsAdd()) > 1 {
			return errors.New("MUTEX attribute accepts at most one tag in tags_add per event")
		}
		return nil
	case catalogv1.DataType_NOT_MUTEX:
		if ev.GetTag() == nil {
			return errors.New("NOT_MUTEX attribute requires a tag payload")
		}
		return nil
	case catalogv1.DataType_PARTIAL_VALUE:
		if ev.GetValue() == nil {
			return errors.New("PARTIAL_VALUE attribute requires a value payload")
		}
		return nil
	case catalogv1.DataType_PARTIAL_VALUE_BY_TAG:
		if ev.GetTagValue() == nil {
			return errors.New("PARTIAL_VALUE_BY_TAG attribute requires a tag_value payload")
		}
		return nil
	case catalogv1.DataType_DATA_TYPE_UNSPECIFIED:
		return errors.New("attribute data_type is unspecified")
	default:
		return fmt.Errorf("unsupported data_type %v", dt)
	}
}
