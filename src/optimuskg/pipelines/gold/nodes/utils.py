class Node:
    """
    Base class for nodes.
    """

    def __init__(self):
        self._id = None
        self._label = None
        self._properties = {}

    @property
    def id(self):
        """
        Returns the node id.
        """
        return self._id

    @property
    def label(self):
        """
        Returns the node label.
        """
        return self._label

    @property
    def properties(self):
        """
        Returns the node properties.
        """
        return self._properties


class Interaction:
    """
    Base class for interactions.
    """

    def __init__(self):
        self._id = None
        self._source_id = None
        self._target_id = None
        self._label = None
        self._properties = {}

    @property
    def id(self):
        """
        Returns the relationship id.
        """
        return self._id

    @property
    def source_id(self):
        """
        Returns the source id.
        """
        return self._source_id

    @property
    def target_id(self):
        """
        Returns the target id.
        """
        return self._target_id

    @property
    def label(self):
        """
        Returns the node label.
        """
        return self._label

    @property
    def properties(self):
        """
        Returns the node properties.
        """
        return self._properties
