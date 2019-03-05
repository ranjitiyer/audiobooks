from ask_sdk_core.dispatch_components import AbstractRequestHandler
from ask_sdk_core.utils import is_request_type, is_intent_name
from ask_sdk_model.interfaces.audioplayer import PlayDirective, AudioItem, \
    Stream, StopDirective
from ask_sdk_model.ui import PlayBehavior
import logging

from src import awsutils

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

HANDLE_TITLE_CONFIRMATION = 'title_confirmation'

"""
Play a random book
"""
class PlayIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("PlayIntent")(handler_input)

    def handle(self, handler_input):
        book = awsutils.get_one_book()
        return play_book(handler_input, book)


class PlaybackStartedHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("AudioPlayer.PlaybackStarted")(handler_input)

    def handle(self, handler_input):
        return handler_input.response_builder.response


class PlaybackFinishedHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("AudioPlayer.PlaybackFinished")(handler_input)

    def handle(self, handler_input):
        return handler_input.response_builder.response


class PlaybackStoppedHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("AudioPlayer.PlaybackStopped")(handler_input)

    def handle(self, handler_input):
        request_dict = handler_input.request_envelope.request.to_dict()
        context = handler_input.request_envelope.context

        userid = context.system.user.user_id
        logger.info("Got user id {}".format(userid))

        # update dynamo
        state = dict()
        state['Offset'] = request_dict['offset_in_milliseconds']
        state['UserId'] = userid
        awsutils.update_offset_in_dynamo(state)

        return handler_input.response_builder.response

"""
Play a book by title
"""
class PlayTitleIntent(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("PlayTitleIntent")(handler_input)

    def handle(self, handler_input):
        log_request(handler_input)
        log_session(handler_input)
        log_context(handler_input)

        # get the slot
        slots = handler_input.request_envelope.request.intents.slots
        if 'title' in slots:
            title = slots['title']

            # set title attribute
            set_attribute('title', title)

            # set dialog_sequence
            set_dialog_sequence_title('title_confirmation')

            # ask for confirmation
            speech = reprompt = 'Did you say {}?'.format(title)
            handler_input.response_builder.speak(speech).ask(reprompt)
            return handler_input.response_builder.response

"""
Utility function to play audio
"""
def play_book(handler_input, book):
    print("Book from S3 {}".format(book))

    logger.info(handler_input.request_envelope.to_dict())

    section = book["sections"]["1"]
    if section:
        url = section['url']
        title = section['name']

        state = dict()
        state['BookId'] = str(book['id'])
        state['Offset'] = 0
        state['SectionNumber'] = 1
        state['Title'] = title
        state['Url'] = url

        userid = handler_input.request_envelope.session.user.user_id
        state['UserId'] = userid

        logger.info("State {}".format(state))

        set_attribute(handler_input, 'userid', userid)

        # persist initial state to Dynamo (Eventually on a separate thread)
        awsutils.save_to_dynamo(state)

        handler_input.response_builder.add_directive(
            PlayDirective(
                play_behavior=PlayBehavior.REPLACE_ALL,
                audio_item=AudioItem(
                    stream=Stream(
                        token=url,
                        url=url,
                        offset_in_milliseconds=0,
                        expected_previous_token=None)
                )
            )
        ).set_should_end_session(True)

        return handler_input.response_builder.response


def log_request(handler_input):
    logger.info("Request {}".format(handler_input.request_envelope.request))

def log_context(handler_input):
    if handler_input.request_envelope.context is not None:
        logger.info("Context {}".format(handler_input.request_envelope.context))
    else:
        logger.info("Context is NONE")

def log_session(handler_input):
    if handler_input.request_envelope.session is not None:
        logger.info("Session {}".format(handler_input.request_envelope.session))
    else:
        logger.info("Session is NONE")

class YesIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name('AMAZON.YesIntent')(handler_input)

    def handle(self, handler_input):
        if HANDLE_TITLE_CONFIRMATION is get_dialog_sequence_title(handler_input):
            # Get the title
            title = get_attribute(handler_input, 'title')

            # Search for the book
            book = next(awsutils.query_by_title(title))

            # get the book by title and play it
            play_book(handler_input, book)

        # page through the results
        elif 'titles' in handler_input.attributes_manager.session_attributes:
            search_titles = handler_input.attributes_manager.session_attributes['titles']

            #TODO:
            # fill the first 5 titles in the response and tell the user
            # there are X more and if they want the next

def get_dialog_sequence_title(handler_input):
    return handler_input.attributes_manager.session_attributes['dialog_sequence']

def set_attribute(handler_input, name, value):
    handler_input.attributes_manager.session_attributes[name] = value

def get_attribute(handler_input, name):
    return handler_input.attributes_manager.session_attributes[name]

def set_dialog_sequence_title(handler_input, title):
    handler_input.attributes_manager.session_attributes['dialog_sequence'] \
        = title


"""
User has confirmed the title. Search
and return results.

If exact match, send the title and ask for 
confirmation to play

If keyword match, 
speak first N results,
send all results in session attributes map
ask if they want to hear more results
"""
def handle_title_confirmation(title, handler_input):
    # search by title
    search_titles = []
    for title in awsutils.query_by_title(title):
        search_titles.append(title)

    # put all titles in the attribute map
    handler_input.attributes_manager.session_attributes['titles'] \
        = search_titles

    # I found one title
    speech = reprompt = ""
    if len(search_titles) == 0:
        speech = reprompt = "Sorry I found no results for {}. Please say another title".format(title)
    if len(search_titles) == 1:
        speech = reprompt = "I found {}. Do you want me to play it?"\
            .format(search_titles[0])
        handler_input.attributes_manager.session_attributes['dialog_sequence'] \
            = 'confirmation to play title'
    else:
        speech = reprompt = 'I found {} titles '.format(len(search_titles))
        handler_input.response_builder.speak(speech).ask(reprompt)
        handler_input.attributes_manager.session_attributes['index'] = 0

    handler_input.response_builder.speak(speech).ask(reprompt)
    return handler_input.response_builder.response

def confirmation_to_play_title(title, handler_input):
    pass

"""

"""
def handle_response_paging():
    pass

class PlaybackNearlyFinishedHandler(AbstractRequestHandler):
    """AudioPlayer.PlaybackNearlyFinished Directive received.
    Replacing queue with the URL again. This should not happen on live streams.
    """
    def can_handle(self, handler_input):
        # type: (HandlerInput) -> bool
        return is_request_type("AudioPlayer.PlaybackNearlyFinished")(handler_input)

    def handle(self, handler_input):
        # type: (HandlerInput) -> Response
        logger.info("In PlaybackNearlyFinishedHandler")

        return handler_input.response_builder.response
        #request = handler_input.request_envelope.request
        # return util.play_later(
        #     url=util.audio_data(request)["url"],
        #     card_data=util.audio_data(request)["card"],
        #     response_builder=handler_input.response_builder)


class PlaybackFailedHandler(AbstractRequestHandler):
    """AudioPlayer.PlaybackFailed Directive received.
    Logging the error and restarting playing with no output speech and card.
    """
    def can_handle(self, handler_input):
        # type: (HandlerInput) -> bool
        return is_request_type("AudioPlayer.PlaybackFailed")(handler_input)

    def handle(self, handler_input):
        # type: (HandlerInput) -> Response
        logger.info("In PlaybackFailedHandler")
        request = handler_input.request_envelope.request
        logger.info("Playback failed: {}".format(request.error))
        return handler_input.response_builder.response

        # return util.play(
        #     url=util.audio_data(request)["url"], offset=0, text=None,
        #     card_data=None,
        #     response_builder=handler_input.response_builder)

class ResumeHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("AMAZON.ResumeIntent")(handler_input)

    def handle(self, handler_input):
        # get state from Dynamo
        item = awsutils.get_offset(handler_input.request_envelope.session.user.user_id)

        handler_input.response_builder.add_directive(
            PlayDirective(
                play_behavior=PlayBehavior.REPLACE_ALL,
                audio_item=AudioItem(
                    stream=Stream(
                        token=item['Url'],
                        url=item['Url'],
                        offset_in_milliseconds=item['Offset'],
                        expected_previous_token=None)
                )
            )
        ).set_should_end_session(True)
        return handler_input.response_builder.response

class PauseIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("AMAZON.PauseIntent")(handler_input)

    def handle(self, handler_input):
        logger.info("In PauseIntentHandle")
        logger.info(handler_input.request_envelope.request.__repr__())
        handler_input.response_builder.add_directive(StopDirective())
        return handler_input.response_builder.response

class ExceptionEncounteredHandler(AbstractRequestHandler):
    """Handler to handle exceptions from responses sent by AudioPlayer
    request.
    """
    def can_handle(self, handler_input):
        # type; (HandlerInput) -> bool
        return is_request_type("System.ExceptionEncountered")(handler_input)

    def handle(self, handler_input):
        # type: (HandlerInput) -> Response
        logger.info("\n**************** EXCEPTION *******************")
        logger.info(handler_input.request_envelope)
        return handler_input.response_builder.response