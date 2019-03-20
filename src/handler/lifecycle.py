from ask_sdk_core.dispatch_components import AbstractRequestHandler, \
    AbstractExceptionHandler, AbstractRequestInterceptor
from ask_sdk_core.utils import is_request_type, is_intent_name

class LaunchRequestHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("LaunchRequest")(handler_input)

    def handle(self, handler_input):
        # speech_text = "Welcome to Librivox on Amazon Echo. You can say Play a book, " \
        #               "or say, Play title, followed by the title name."

        speech_text = "Welcome to Librivox on Amazon Echo."

        handler_input.response_builder.speak(speech_text) \
            .set_should_end_session(False)

        return handler_input.response_builder.response


class HelpIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("AMAZON.HelpIntent")(handler_input)

    def handle(self, handler_input):
        speech_text = "You can say hello"

        handler_input.response_builder.speak(speech_text)
        return handler_input.response_builder.response


class CancelAndStopIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("AMAZON.CancelIntent")(handler_input) \
                or is_intent_name("AMAZON.StopIntent")(handler_input)

    def handle(self, handler_input):
        speech_text = "Goodbye!"

        handler_input.response_builder.speak(speech_text)
        return handler_input.response_builder.response


class SessionEndedIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("SessionEndedRequest")(handler_input)

    def handle(self, handler_input):
        # clean up logic goes here
        return handler_input.response_builder.response



class AllExceptionHandler(AbstractExceptionHandler):
    def can_handle(self, handler_input, exception):
        return True

    def handle(self, handler_input, exception):
        print(exception.__repr__())
        print("Sorry I didn't get it, can you please say it again?")
        speech_text = "Sorry I didn't get it, can you please say it again?"
        handler_input.response_builder.speak(speech_text).ask(speech_text)
        return handler_input.response_builder.response


class GreetingIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        print(handler_input.__repr__())
        return is_intent_name("GreetingIntent")(handler_input)

    def handle(self, handler_input):
        speech_text = "A very good afternoon on this beautiful and sunny Sunday!!"

        print(handler_input.context.__repr__())

        handler_input.response_builder.speak(speech_text) \
            .set_should_end_session(False)

        return handler_input.response_builder.response


class LoggingRequestInterceptor(AbstractRequestInterceptor):
    def process(self, handler_input):
        request = handler_input.request_envelope.request
        session = handler_input.request_envelope.session
        context = handler_input.request_envelope.context

        print("Request : {}".format(request.__repr__()))
        print("Session : {}".format(session.__repr__()))
        print("Session : {}".format(context.__repr__()))
