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
