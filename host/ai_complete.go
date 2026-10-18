package host

import (
	"context"
	"errors"
	"strings"
	"time"
)

// AIErrorKind classifies why an AI turn could not run, so a plugin can show
// its own localized message.
type AIErrorKind string

const (
	AIErrNone         AIErrorKind = ""
	AIErrRateLimited  AIErrorKind = "rate_limited" // hourly turn limit of the plugin
	AIErrBusy         AIErrorKind = "busy"         // too many turns running for the plugin
	AIErrNoCapability AIErrorKind = "no_capability"
	AIErrUnsupported  AIErrorKind = "unsupported" // host has no host.ai.turn.* methods
	AIErrFailed       AIErrorKind = "failed"      // the turn ended with an error
	AIErrCancelled    AIErrorKind = "cancelled"   // the turn was cancelled outside the plugin
	AIErrLost         AIErrorKind = "lost"        // the host forgot the turn
)

// AITurnError is returned by CompleteAITurn when a turn ends without a reply.
type AITurnError struct {
	Kind    AIErrorKind
	Message string
}

func (e *AITurnError) Error() string {
	if e.Message == "" {
		return "ai turn " + string(e.Kind)
	}
	return "ai turn " + string(e.Kind) + ": " + e.Message
}

// ClassifyAIError maps errors of StartAITurn / CompleteAITurn to a kind.
// Context errors and unknown errors return AIErrNone.
func ClassifyAIError(err error) AIErrorKind {
	if err == nil {
		return AIErrNone
	}
	var te *AITurnError
	if errors.As(err, &te) {
		return te.Kind
	}
	var he *Error
	if !errors.As(err, &he) {
		return AIErrNone
	}
	switch {
	case strings.Contains(he.Message, "hourly AI turn limit"):
		return AIErrRateLimited
	case strings.Contains(he.Message, "too many AI turns"):
		return AIErrBusy
	case he.Code == CodeCapabilityMissing:
		return AIErrNoCapability
	case he.Code == -32601:
		return AIErrUnsupported
	case he.Code == -32004:
		return AIErrLost
	}
	return AIErrNone
}

// AIErrorMessage returns a short Chinese message for errors of StartAITurn /
// CompleteAITurn, so official plugins show the same wording; context errors
// and unknown errors keep their own text.
func AIErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "已取消"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "AI 回复超时，请重试"
	}
	switch ClassifyAIError(err) {
	case AIErrRateLimited:
		return "本小时的 AI 次数已用完，请稍后再试"
	case AIErrBusy:
		return "AI 请求过多，请等当前回答完成后再试"
	case AIErrNoCapability:
		return "插件未获得 AI 能力，请在插件管理中允许 AI 后重试"
	case AIErrUnsupported:
		return "当前 MinCode 版本不支持插件调用 AI，请升级"
	case AIErrCancelled:
		return "AI 回合已被取消"
	case AIErrLost:
		return "AI 回合已结束，请重试"
	case AIErrFailed:
		var te *AITurnError
		if errors.As(err, &te) && te.Message != "" {
			return "AI 回答失败：" + te.Message
		}
		return "AI 回答失败"
	}
	var he *Error
	if errors.As(err, &he) && he.Message != "" {
		return he.Message
	}
	return err.Error()
}

// CompleteAITurn starts a turn, polls it every poll (default 400ms) until it
// ends and returns the final text. partial, when set, receives the full text
// so far whenever it changes. Cancelling ctx cancels the turn.
func CompleteAITurn(ctx context.Context, c Caller, request AITurnRequest, poll time.Duration, partial func(text string)) (string, error) {
	if poll <= 0 {
		poll = 400 * time.Millisecond
	}
	state, err := StartAITurn(ctx, c, request)
	if err != nil {
		return "", err
	}
	last, pollErrors := "", 0
	for {
		if state.Text != last && partial != nil {
			last = state.Text
			partial(last)
		}
		if state.Done {
			switch state.Status {
			case AITurnCompleted:
				return state.Text, nil
			case AITurnCancelled:
				if ctx.Err() != nil {
					return "", ctx.Err()
				}
				return "", &AITurnError{Kind: AIErrCancelled}
			default:
				return "", &AITurnError{Kind: AIErrFailed, Message: strings.TrimSpace(state.Error)}
			}
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_, _ = CancelAITurn(cctx, c, state.ID)
			cancel()
			return "", ctx.Err()
		case <-timer.C:
		}
		next, err := PollAITurn(ctx, c, state.ID)
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			if IsCode(err, -32004) {
				return "", &AITurnError{Kind: AIErrLost}
			}
			if pollErrors++; pollErrors > 3 {
				return "", err
			}
			continue
		}
		pollErrors = 0
		state = next
	}
}
