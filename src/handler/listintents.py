from ask_sdk_core.dispatch_components import AbstractRequestHandler
from ask_sdk_core.utils import is_request_type, is_intent_name
from ask_sdk_model import Slot

from src.awsutils import get_all_genres, get_all_authors, query_by_genre, \
    query_by_author

LIST_GENRES = 'ListAllGenresIntent'
LIST_AUTHORS = 'ListAllAuthorsIntent'
LIST_BY_AUTHOR = 'ListTitleByAuthorIntent'
LIST_BY_GENRE = 'ListTitleByGenreIntent'

class ListGenresHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name(LIST_GENRES)(handler_input)

    def handle(self, handler_input):
        # query all genres
        all_genres: list = get_all_genres()
        print("Genres being returned {}".format(all_genres))

        speech_text = "Here are the first five genres I found. {}"\
            .format(", ".join(all_genres[:5]))

        speech_text = "Here are the first five genres I found. {}".format\
            (all_genres[:1])

        #print("{}".format(speech_text))

        handler_input.response_builder.speak(speech_text) \
            .set_should_end_session(False)

        return handler_input.response_builder.response

# Needs dialog confirmation
class ListTitlesByGenre(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name(LIST_BY_GENRE)(handler_input)

    def handle(self, handler_input):
        # get the slot
        slots = handler_input.request_envelope.request.intent.slots
        if 'genre' in slots:
            slot: Slot = slots['genre']
            genre = slot.value.lower()

            all_titles: list = []
            for title in query_by_genre(genre):
                all_titles.append(title['title'])

            speech_text = "Here are some titles I found. {}"\
                .format(",".join(all_titles[:5]))
            handler_input.response_builder.speak(speech_text) \
                .set_should_end_session(False)

            return handler_input.response_builder.response

# Needs dialog confirmation
# if author is two words, might need to search on
# each token if the the full name search does not match
class ListTitlesByAuthor(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name(LIST_BY_AUTHOR)(handler_input)

    def handle(self, handler_input):
        print("In the handler for ListTitles by author")
        # get the slot
        slots = handler_input.request_envelope.request.intent.slots

        print(slots)

        if 'author' in slots:
            slot: Slot = slots['author']
            author = slot.value.lower()

            print("Got author in slot {}".format(author))

            all_titles: list = []
            for title in query_by_author(author):
                all_titles.append(title['title'])

            speech_text = "Here are some titles I found. {}"\
                .format(",".join(all_titles[:5]))
            handler_input.response_builder.speak(speech_text) \
                .set_should_end_session(False)

            return handler_input.response_builder.response


class ListAuthorsHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name(LIST_AUTHORS)(handler_input)

    def handle(self, handler_input):
        # query all genres
        all_authors: list = get_all_authors()
        print("Genres being returned {}".format(all_authors))

        speech_text = "Here are the first five authors I found. {}"\
            .format(",".join(all_authors[:5]))

        print("Speech text is {}".format(speech_text))

        handler_input.response_builder.speak(speech_text) \
            .set_should_end_session(False)

        return handler_input.response_builder.response
