package claim

import "sync"

type userEventBroker struct {
	mu           sync.Mutex
	nextID       int
	entriesByUID map[int64]*userEventEntry
}

type userEventEntry struct {
	version       int
	subscriptions map[int]*userEventSubscription
}

type userEventSubscription struct {
	userID int64
	id     int
	ch     chan int
}

type queueStatusEventBroker = userEventBroker
type queueStatusSubscription = userEventSubscription

type claimRealtimeEventBroker = userEventBroker
type claimRealtimeSubscription = userEventSubscription

type uploadResultsEventBroker = userEventBroker
type uploadResultsSubscription = userEventSubscription

func newUserEventBroker() *userEventBroker {
	return &userEventBroker{
		entriesByUID: make(map[int64]*userEventEntry),
	}
}

func newQueueStatusEventBroker() *queueStatusEventBroker {
	return newUserEventBroker()
}

func newClaimRealtimeEventBroker() *claimRealtimeEventBroker {
	return newUserEventBroker()
}

func newUploadResultsEventBroker() *uploadResultsEventBroker {
	return newUserEventBroker()
}

func (b *userEventBroker) subscribe(userID int64) (*userEventSubscription, int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	entry := b.entriesByUID[userID]
	if entry == nil {
		entry = &userEventEntry{
			subscriptions: make(map[int]*userEventSubscription),
		}
		b.entriesByUID[userID] = entry
	}

	b.nextID++
	subscription := &userEventSubscription{
		userID: userID,
		id:     b.nextID,
		ch:     make(chan int, 1),
	}
	entry.subscriptions[subscription.id] = subscription
	return subscription, entry.version
}

func (b *userEventBroker) unsubscribe(subscription *userEventSubscription) {
	if subscription == nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	entry := b.entriesByUID[subscription.userID]
	if entry == nil {
		return
	}
	delete(entry.subscriptions, subscription.id)
	if len(entry.subscriptions) == 0 {
		delete(b.entriesByUID, subscription.userID)
	}
}

func (b *userEventBroker) notify(userID int64) int {
	b.mu.Lock()
	entry := b.entriesByUID[userID]
	if entry == nil || len(entry.subscriptions) == 0 {
		b.mu.Unlock()
		return 0
	}
	entry.version++
	version := entry.version
	subscriptions := make([]*userEventSubscription, 0, len(entry.subscriptions))
	for _, subscription := range entry.subscriptions {
		subscriptions = append(subscriptions, subscription)
	}
	b.mu.Unlock()

	for _, subscription := range subscriptions {
		pushLatestVersion(subscription.ch, version)
	}
	return version
}

func pushLatestVersion(ch chan int, version int) {
	for {
		select {
		case ch <- version:
			return
		default:
			select {
			case <-ch:
			default:
				return
			}
		}
	}
}
