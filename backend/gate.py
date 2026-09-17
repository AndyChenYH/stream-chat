import asyncio
from collections import deque
from dataclasses import dataclass


class QueueFull(Exception):
    pass


class ConversationBusy(Exception):
    pass


@dataclass(eq=False)
class Ticket:
    conversation_id: str
    ready: asyncio.Event


class Gate:
    """One active generation and a bounded FIFO queue; one event loop/process only."""

    def __init__(self, waiting_limit=5):
        self.limit = waiting_limit
        self.tickets = deque()

    def reserve(self, conversation_id):
        if any(t.conversation_id == conversation_id for t in self.tickets):
            raise ConversationBusy
        if len(self.tickets) >= self.limit + 1:
            raise QueueFull
        ticket = Ticket(conversation_id, asyncio.Event())
        self.tickets.append(ticket)
        if len(self.tickets) == 1:
            ticket.ready.set()
        return ticket

    def release(self, ticket):
        if ticket in self.tickets:
            self.tickets.remove(ticket)
        if self.tickets:
            self.tickets[0].ready.set()

    def position(self, ticket):
        return list(self.tickets).index(ticket)
