package loamstream

import loamstream.loam.LoamGraph

package object compiler {
  type GraphThunk = () => LoamGraph
}
