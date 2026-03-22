package claim

import "sync"

type queueStatusEventBroker struct {
	mu           sync.Mutex
	nextID       int
	entriesByUID map[int64]*queueStatusEventEntry
}

type queueStatusEventEntry struct {
	version       int
	subscriptions map[int]*queueStatusSubscription
}

type queueStatusSubscription struct {
	userID int64
	id     int
	ch     chan int
}

func newQueueStatusEventBroker() *queueStatusEventBroker {
	return &queueStatusEventBroker{
		entriesByUID: make(map[int64]*queueStatusEventEntry),
	}
}

func (b *queueStatusEventBroker) subscribe(userID int64) (*queueStatusSubscription, int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	entry := b.entriesByUID[userID]
	if entry == nil {
		entry = &queueStatusEventEntry{
			subscriptions: make(map[int]*queueStatusSubscription),
		}
		b.entriesByUID[userID] = entry
	}

	b.nextID++
	subscription := &queueStatusSubscription{
		userID: userID,
		id:     b.nextID,
		ch:     make(chan int, 1),
	}
	entry.subscriptions[subscription.id] = subscription
	return subscription, entry.version
}

func (b *queueStatusEventBroker) unsubscribe(subscription *queueStatusSubscription) {
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

func (b *queueStatusEventBroker) notify(userID int64) int {
	b.mu.Lock()
	entry := b.entriesByUID[userID]
	if entry == nil || len(entry.subscriptions) == 0 {
		b.mu.Unlock()
		return 0
	}
	entry.version++
	version := entry.version
	subscriptions := make([]*queueStatusSubscription, 0, len(entry.subscriptions))
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
