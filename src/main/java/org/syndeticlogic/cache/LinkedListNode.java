package org.syndeticlogic.cache;
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
/**
 * Class LinkedListNode
 *
 *
 * @author <a href="mailto:jeff@shiftone.org">Jeff Drost</a>
 * @version $Revision: 1.6 $
 */
public class LinkedListNode
{

    LinkedListNode next;
    LinkedListNode prev;
    Object         value;

    public LinkedListNode(Object value)
    {
        this.value = value;
    }


    /**
     * Method getValue
     */
    public Object getValue()
    {
        return value;
    }


    /**
     * Method getNext
     */
    public LinkedListNode getNext()
    {

        return (next.isHeaderNode()
                ? null
                : next);
    }


    /**
     * Method getPrevious
     */
    public LinkedListNode getPrevious()
    {

        return (prev.isHeaderNode()
                ? null
                : prev);
    }


    /**
     * is this node the header node in a linked list?
     */
    boolean isHeaderNode()
    {
        return (value == this);
    }
}
