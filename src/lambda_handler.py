from ask_sdk_core.skill_builder import SkillBuilder

from src.handler.audio import ExceptionEncounteredHandler, PauseIntentHandler, \
    PlayIntentHandler, PlaybackFinishedHandler, PlaybackStoppedHandler, \
    PlaybackNearlyFinishedHandler, PlaybackStartedHandler, \
    PlaybackFailedHandler, ResumeHandler, PlayTitleIntent
from src.handler.lifecycle import LaunchRequestHandler, HelpIntentHandler, \
    CancelAndStopIntentHandler, SessionEndedIntentHandler, \
    GreetingIntentHandler, LoggingRequestInterceptor, AllExceptionHandler
from src.handler.listintents import ListGenresHandler, ListAuthorsHandler, \
    ListTitlesByAuthor, ListTitlesByGenre

sb = SkillBuilder()

sb.add_request_handler(ExceptionEncounteredHandler())
sb.add_request_handler(LaunchRequestHandler())
sb.add_request_handler(HelpIntentHandler())
sb.add_request_handler(CancelAndStopIntentHandler())
sb.add_request_handler(SessionEndedIntentHandler())
sb.add_request_handler(GreetingIntentHandler())
sb.add_request_handler(PauseIntentHandler())
sb.add_request_handler(PlayTitleIntent())

sb.add_request_handler(PlayIntentHandler())
sb.add_request_handler(PlaybackFinishedHandler())
sb.add_request_handler(PlaybackStoppedHandler())
sb.add_request_handler(PlaybackNearlyFinishedHandler())
sb.add_request_handler(PlaybackStartedHandler())
sb.add_request_handler(PlaybackFailedHandler())
sb.add_request_handler(ResumeHandler())

# list genres and authors
sb.add_request_handler(ListGenresHandler())
sb.add_request_handler(ListAuthorsHandler())

# list titles by genre and author
sb.add_request_handler(ListTitlesByAuthor())
sb.add_request_handler(ListTitlesByGenre())

# sb.add_global_request_interceptor(LoggingRequestInterceptor())
sb.add_exception_handler(AllExceptionHandler())

handler = sb.lambda_handler()