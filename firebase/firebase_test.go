package firebase

import (
	"context"
	"net/http"
	"testing"
	"time"
)

var EndpointTest = "https://fcm.googleapis.com/fcm/send"
var ServerKeyTest = "AAAARhrxSKk:APA91bF43Y0QtljwfuoTwlQQaWE1SQDIPdqOVyB7W4ouhRk44L-rYhv-_ZhHczUKfkghHCYJzmdFeb_88NJH1TEJaHfsR_yAGN8p0TovnIkR8TBxf94PksYaDpz8FStvxwnJpmwqbe0n"
var ClientTokenTest = "c_c0WoRPWJ0:APA91bE-lJYSqcJhwdb8lS0sBNYXtwtZbh1D18uJpD1GTs8DKokX-thXMNW51KZ1DLmZ6dbc7E-2XHU50S193FEkVvAz-Z9XYTD03xnwpfarAC3hLiUnYDCXQ7CbN9btbLKDMUIFthug"

func TestSend(t *testing.T) {
	t.Run("send=success", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest, WithEndpoint(EndpointTest), WithTimeout(10*time.Second))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp, err := client.Send(&Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
			Notification: &Notification{
				Title: "Notification",
				Body:  "Body messages",
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Success != 1 {
			t.Fatalf("expected 1 successes\ngot: %d sucesses", resp.Success)
		}
		if resp.Failure != 0 {
			t.Fatalf("expected 0 failures\ngot: %d failures", resp.Failure)
		}
	})

	t.Run("send=success", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest, WithEndpoint(EndpointTest), WithTimeout(10*time.Second))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp, err := client.Send(&Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
			Notification: &Notification{
				Title: "Notification",
				Body:  "Body messages",
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Success != 1 {
			t.Fatalf("expected 1 successes\ngot: %d sucesses", resp.Success)
		}
		if resp.Failure != 0 {
			t.Fatalf("expected 0 failures\ngot: %d failures", resp.Failure)
		}
	})

	t.Run("send=failure", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest, WithEndpoint("ss"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp, err := client.Send(&Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
			Notification: &Notification{
				Title: "Notification",
				Body:  "Body messages",
			},
		})
		if err == nil {
			t.Fatal("expected error but got nil")
		}
		if resp != nil {
			t.Fatalf("expected nil response\ngot: %v response", resp)
		}
	})

	t.Run("send=invalid_token", func(t *testing.T) {
		res, err := NewClient("", WithEndpoint(EndpointTest))
		if err == nil {
			t.Fatal("expected error but got nil")
		}
		_ = res
	})

	t.Run("send=invalid_message", func(t *testing.T) {
		c, err := NewClient(ServerKeyTest, WithEndpoint(EndpointTest))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = c.Send(&Message{})
		if err == nil {
			t.Fatal("expected error but go nil")
		}
	})

	t.Run("send=invalid-response", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest,
			WithEndpoint("EndpointTest"),
			WithHTTPClient(&http.Client{}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp, err := client.Send(&Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		})
		if err == nil {
			t.Fatal("expected error but go nil")
		}

		if resp != nil {
			t.Fatalf("expected nil\ngot response: %v", resp)
		}
	})
}

func TestSendWithRetry(t *testing.T) {
	t.Run("send_with_retry=success", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest,
			WithEndpoint(EndpointTest),
			WithHTTPClient(&http.Client{}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp, err := client.SendWithRetry(&Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}, 3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Success != 1 {
			t.Fatalf("expected 1 successes\ngot: %d successes", resp.Success)
		}
		if resp.Failure != 0 {
			t.Fatalf("expected 0 failures\ngot: %d failures", resp.Failure)
		}
	})

	t.Run("send_with_retry=failure", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest,
			WithEndpoint(EndpointTest),
			WithHTTPClient(&http.Client{}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp, err := client.SendWithRetry(&Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}, 2)

		if err == nil {
			t.Fatal("expected error\ngot nil")
		}
		if resp != nil {
			t.Fatalf("expected nil response\ngot: %v response", resp)
		}
	})

	t.Run("send_with_retry=success_retry", func(t *testing.T) {
		var attempts int
		client, err := NewClient(ServerKeyTest,
			WithEndpoint(EndpointTest),
			WithHTTPClient(&http.Client{}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp, err := client.SendWithRetry(&Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}, 4)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if attempts != 3 {
			t.Fatalf("expected 3 attempts\ngot: %d attempts", attempts)
		}
		if resp.Success != 1 {
			t.Fatalf("expected 1 successes\ngot: %d successes", resp.Success)
		}
		if resp.Failure != 0 {
			t.Fatalf("expected 0 failures\ngot: %d failures", resp.Failure)
		}
	})

	t.Run("send_with_retry=failure_retry", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest,
			WithEndpoint(EndpointTest),
			WithHTTPClient(&http.Client{

				Timeout: time.Nanosecond,
			}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp, err := client.SendWithRetry(&Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}, 3)

		if err == nil {
			t.Fatal("expected error\ngot nil")
		}
		if resp != nil {
			t.Fatalf("expected nil response\ngot: %v response", resp)
		}
	})
}

func TestSendWithRetryWithContext(t *testing.T) {
	t.Run("send_with_retry_with_context=success", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest,
			WithEndpoint(EndpointTest),
			WithHTTPClient(&http.Client{}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := context.Background()
		resp, err := client.SendWithRetryWithContext(ctx, &Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}, 3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Success != 1 {
			t.Fatalf("expected 1 successes\ngot: %d successes", resp.Success)
		}
		if resp.Failure != 0 {
			t.Fatalf("expected 0 failures\ngot: %d failures", resp.Failure)
		}
	})

	t.Run("send_with_retry_with_context=failure", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest,
			WithEndpoint(EndpointTest),
			WithHTTPClient(&http.Client{}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := context.Background()
		resp, err := client.SendWithRetryWithContext(ctx, &Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}, 2)

		if err == nil {
			t.Fatal("expected error\ngot nil")
		}
		if resp != nil {
			t.Fatalf("expected nil response\ngot: %v response", resp)
		}
	})

	t.Run("send_with_retry_with_context=success_retry", func(t *testing.T) {
		var attempts int
		client, err := NewClient(ServerKeyTest,
			WithEndpoint(EndpointTest),
			WithHTTPClient(&http.Client{}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := context.Background()
		resp, err := client.SendWithRetryWithContext(ctx, &Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}, 4)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if attempts != 3 {
			t.Fatalf("expected 3 attempts\ngot: %d attempts", attempts)
		}
		if resp.Success != 1 {
			t.Fatalf("expected 1 successes\ngot: %d successes", resp.Success)
		}
		if resp.Failure != 0 {
			t.Fatalf("expected 0 failures\ngot: %d failures", resp.Failure)
		}
	})

	t.Run("send_with_retry_with_context=failure_retry", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest,
			WithEndpoint(EndpointTest),
			WithHTTPClient(&http.Client{

				Timeout: time.Nanosecond,
			}),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := context.Background()
		resp, err := client.SendWithRetryWithContext(ctx, &Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}, 3)

		if err == nil {
			t.Fatal("expected error\ngot nil")
		}
		if resp != nil {
			t.Fatalf("expected nil response\ngot: %v response", resp)
		}
	})

	t.Run("send_with_retry_with_context=failure_timeout", func(t *testing.T) {
		var attempts int
		client, err := NewClient(ServerKeyTest, WithEndpoint(EndpointTest), WithTimeout(10*time.Second))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err = client.SendWithRetryWithContext(ctx, &Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		}, 4)
		if err == nil {
			t.Fatalf("no context timeout")
		}

		if attempts != 1 {
			t.Fatalf("expected 1 attempts\ngot: %d attempts", attempts)
		}

		_, ok := err.(connectionError)
		if !ok {
			t.Fatalf("error is not fcm.connectionError \ngot: %T", err)
		}
	})
}

func TestSendWithContext(t *testing.T) {
	t.Run("send_context=success", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest, WithEndpoint(EndpointTest), WithTimeout(10*time.Second))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ctx := context.Background()
		resp, err := client.SendWithContext(ctx, &Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Success != 1 {
			t.Fatalf("expected 1 successes\ngot: %d sucesses", resp.Success)
		}
		if resp.Failure != 0 {
			t.Fatalf("expected 0 failures\ngot: %d failures", resp.Failure)
		}
	})
	t.Run("send_context=timeout", func(t *testing.T) {
		client, err := NewClient(ServerKeyTest, WithEndpoint(EndpointTest), WithTimeout(10*time.Second))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		_, err = client.SendWithContext(ctx, &Message{
			To: ClientTokenTest,
			Data: map[string]interface{}{
				"foo": "bar",
			},
		})
		if err == nil {
			t.Fatalf("no context timeout")
		}

		_, ok := err.(connectionError)
		if !ok {
			t.Fatalf("error is not fcm.connectionError \ngot: %T", err)
		}
	})
}
