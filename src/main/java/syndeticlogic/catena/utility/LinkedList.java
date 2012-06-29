package syndeticlogic.catena.utility;

/*
 * Author == James Percent (james@empty-set.net) 
 *
 * Copyright 2010, 2011 James Percent
 * 
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This linked list is different from the java.util implementation in that it
 * exposes access to the nodes themselves, allowing lower level manipulation.
 * This ends up being rather critical when removing elements from a cache.
 * Having a reference to the node allows it to be removed in constant time -
 * rather than having to search for it first.
 * 
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 * @author <a href="mailto:jeff@shiftone.org">Jeff Drost</a>
 * @version $Revision: 1.8 $
 */
public class LinkedList {

	private static Log log = LogFactory.getLog(LinkedList.class);
	private LinkedListNode header = null;
	private int size = 0;

	/**
	 * Constructor LinkedList
	 * 
	 */
	public LinkedList() {

		header = new LinkedListNode(null);
		header.value = header; // this is how I designate the header node
		header.prev = header;
		header.next = header;
	}

	/**
	 * adding an object to the list, making it the new first node. for the
	 * purposes of treating this list as a queue or stack, <b>this</b> is the
	 * end of the list that should be used when adding.
	 */
	public final LinkedListNode addFirst(Object obj) {
		return addBefore(header.next, obj);
	}

	/**
	 * adding an object to the list, making it the new last node.
	 */
	public final LinkedListNode addLast(Object obj) {
		return addBefore(header, obj);
	}

	/**
	 * remove a node from the end of the list (list is being used like a
	 * <b>queue</b>).
	 */
	public final Object dequeue() {
		return removeLast();
	}

	/**
	 * remove a node from the beginning of the list (list is being used like a
	 * <b>stack</b>)
	 */
	public final Object pop() {
		return removeFirst();
	}

	/**
	 * peek the first element without removing it. This is a <b>stack</b> style
	 * peek
	 */
	public final LinkedListNode peekFirst() {

		return (header.next == header) ? null : header.next;
	}

	/**
	 * peek the last element without removing it. This is a <b>queue</b> style
	 * peek
	 */
	public final LinkedListNode peekLast() {

		return (header.prev == header) ? null : header.prev;
	}

	/**
	 * Method removeFirst
	 */
	private final Object removeFirst() {
		return remove(header.next);
	}

	/**
	 * Method removeLast
	 */
	private final Object removeLast() {
		return remove(header.prev);
	}

	/**
	 * promotes this node to the front of the the list.
	 */
	public final void moveToFirst(LinkedListNode node) {
		remove(node);
		addNodeBefore(header.next, node);
	}

	/**
	 * demotes this node to the end of the list.
	 */
	public final void moveToLast(LinkedListNode node) {
		remove(node);
		addNodeBefore(header, node);
	}

	/**
	 * Method addBefore (somewhat expensive - alloc)
	 */
	public final LinkedListNode addBefore(LinkedListNode node, Object obj) {

		LinkedListNode newNode = new LinkedListNode(obj);

		addNodeBefore(node, newNode);

		return newNode;
	}

	/**
	 * Method addNodeBefore
	 */
	public final void addNodeBefore(LinkedListNode nodeToAddBefore,
			LinkedListNode newPreviousNode) {

		newPreviousNode.prev = nodeToAddBefore.prev;
		newPreviousNode.next = nodeToAddBefore;
		newPreviousNode.prev.next = newPreviousNode;
		newPreviousNode.next.prev = newPreviousNode;

		size++;
	}

	/**
	 * Removes the node from the list and returns the value of the former node.
	 */
	public final Object remove(LinkedListNode node) {

		if ((node == null) || (node == header)) {
			return null;
		}

		node.prev.next = node.next;
		node.next.prev = node.prev;
		node.prev = null;
		node.next = null;

		size--;

		return node.value;
	}

	/**
	 * Method size
	 */
	public int size() {
		return size;
	}

	/**
	 * Method toString
	 */
	public String toString() {

		StringBuffer sb = new StringBuffer();
		LinkedListNode thisNode = header.next;

		sb.append("[LIST size=" + size() + "]");

		while (thisNode != header) {
			sb.append("<->");
			sb.append(String.valueOf(thisNode.value));

			thisNode = thisNode.next;
		}

		return sb.toString();
	}

	/**
	 * Method main
	 */
	public static void main(String[] args) {

		LinkedList fifo = new LinkedList();

		for (int i = 0; i < 5; i++) {
			fifo.addFirst("#" + i);
		}

		LinkedListNode a = fifo.addFirst("AAAAA");
		LinkedListNode b = fifo.addFirst("BBBBB");

		for (int i = 5; i < 10; i++) {
			fifo.addFirst("#" + i);
		}

		fifo.moveToFirst(a);
		fifo.moveToLast(b);
		while (fifo.pop() != null) { log.debug(fifo); }
	}
}
