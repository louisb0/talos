from typing import List, Union, Tuple

class CommentCollector:
    """
    Collects all comments from an API response containing comment data.
    """
    def __init__(self, api_response: dict):
        """
        Initialises the CommentCollector.

        Args:
            api_response (str): The API response containing comments.
        """
        self.iterator = CommentIterator(api_response)
    
    def collect_comments(self) -> Tuple[List[dict], ...]:
        """
        Iterates over the comments building lists for each type of comment.

        Returns:
            Tuple[List[dict], ...]: A tuple containing lists of comment objects (rawComments, moreComments, continueThreads).
        """
        raw_comments = []
        more_comments = []
        continue_threads = []

        for comment in self.iterator:
            if comment["type"] == "rawComment":
                raw_comments.append(comment)
            elif comment["type"] == "moreComment":
                more_comments.append(comment)
            elif comment["type"] == "continueThread":
                continue_threads.append(comment)

        return (raw_comments, more_comments, continue_threads)

class CommentIterator:
    """
    Iterates over all comments, including 'moreCommments' or 'continueThreads'
    from an API response.
    """
    def __init__(self, api_response: str):
        """
        Initialises the CommentIterator.

        Args:
            api_response (str): The API response containing comments.
        """
        self.comment_sections = {
            "rawComment": api_response["comments"],
            "moreComment": api_response["moreComments"],
            "continueThread": api_response["continueThreads"],
        }

        self.stack = self._setup_stack()

    def _setup_stack(self) -> List[Union[dict, None]]:
        """
        Finds the 'first' comment in the comment section, allowing for the possibility
        of a bugged comment section containing only 'show more' or 'continue thread'.
        Returns this as a list to setup the stack for traversal.

        Returns:
            List[Union[dict, None]]: List containing the comment, or None if no comment exists.
        """
        for type, comments in self.comment_sections.items():
            if comments is None or len(comments.keys()) == 0:
                continue
            
            first_comment = next(iter(comments.values()))
            first_comment["type"] = type

            return [first_comment]
        return []

    def __iter__(self):
        return self

    def __next__(self) -> dict:
        if not self.stack:
            raise StopIteration
        
        to_return = self.stack.pop()
        next_field = to_return.get("next")

        if next_field:
            next_comment = self._find_comment_from_id(next_field.get("id"))
            self.stack.append(next_comment)

        return to_return

    def _find_comment_from_id(self, id: str) -> dict:
        """
        Given an ID of a comment, returns the comment object in the API response.
        Useful due to presence of moreComments or continueThreads.

        Args:
            id (str): The ID of the comment to find.

        Returns:
            dict: The comment object matching the ID.
        """
        for type, comments in self.comment_sections.items():
            if id in comments:
                next_comment = comments[id]
                next_comment["type"] = type

                return next_comment