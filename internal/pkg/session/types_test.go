package session

import "testing"

func TestSessionKeyIncludesDevice(t *testing.T) {
	// 同一用户多设备的 session 键必须隔离，
	// 否则任一设备断开都会 Destroy 掉其它设备的 session。
	mobile := User{BID: 1, UID: 42, Device: DeviceMobile}
	pc := User{BID: 1, UID: 42, Device: DevicePC}

	if mobile.SessionKey() == pc.SessionKey() {
		t.Fatalf("expected distinct session keys per device, got %s for both", mobile.SessionKey())
	}
	if mobile.ConnKey() != pc.ConnKey() {
		t.Fatalf("expected shared conn key for same user, got %s / %s", mobile.ConnKey(), pc.ConnKey())
	}
}
