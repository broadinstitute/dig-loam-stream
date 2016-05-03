package loamstream.web.controllers

import play.api.mvc.{Action, AnyContent, Controller}

/**
  * LoamStream
  * Created by oliverr on 5/3/2016.
  */
class WebController extends Controller {

  def index: Action[AnyContent] = Action {
    Ok("It works!")
  }

}
