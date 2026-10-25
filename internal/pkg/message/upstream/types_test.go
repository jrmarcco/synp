package upstream

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSanitizeSyncErrorBusinessMessages(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "permission denied surfaces server message",
			err:  status.Error(codes.PermissionDenied, "user is not an active member of the conversation"),
			want: "failed to sync: user is not an active member of the conversation",
		},
		{
			name: "invalid argument surfaces server message",
			err:  status.Error(codes.InvalidArgument, "conversation_id is not a group conversation"),
			want: "failed to sync: conversation_id is not a group conversation",
		},
		{
			name: "internal error is masked",
			err:  status.Error(codes.Internal, "failed to list inbox entries: sql: connection refused"),
			want: "failed to sync: internal error, please retry",
		},
		{
			name: "timeout gets retry hint",
			err:  status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			want: "failed to sync: timed out, please retry",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeSyncError("failed to sync", tc.err)
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
			if strings.Contains(got, "sql:") {
				t.Fatalf("internal detail leaked: %q", got)
			}
		})
	}
}
