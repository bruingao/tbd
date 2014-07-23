/**
 * Copyright (C) 2013 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tbd.visualization

import tbd.ddg.{Node, RootNode, ReadNode, MemoNode, WriteNode, ParNode}
import org.graphstream.graph.implementations.{SingleGraph}
import org.graphstream.ui.swingViewer.Viewer
import scala.collection.mutable.{HashMap, ListBuffer}
import swing._
import GridBagPanel._
import org.graphstream.ui.swingViewer.ViewerListener

class TbdVisualizer extends ViewerListener {

  var highlightRemoved = false
  var showLabels = false
  val graphStyle = """
    node.root {
      size: 20px;
      fill-color: grey;
      shape: box;
    }
    edge {
      arrow-shape: arrow;
    }
    node.read{
      size: 10px;
      fill-color: blue;
    }
    node.memo{
      size: 20px;
      shape: diamond;
      fill-color: LightGreen;
    }
    node.write {
      size: 10px;
      fill-color: orange;
    }
    node.par {
      size: 20px;
      fill-color: yellow;
      shape: diamond;
    }
    node {
      text-alignment: under;
      text-background-color: #EEEEEE;
      text-background-mode: rounded-box;
      text-padding: 2px;
      stroke-mode: none;
      stroke-width: 2;
      stroke-color: green;
    }
  """

  val vSpacing = -50
  val hSpacing = 50

  val graph = new SingleGraph("DDG")
  graph.addAttribute("ui.stylesheet", graphStyle)
  System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")

  var display = new Viewer(graph, Viewer.ThreadingModel.GRAPH_IN_ANOTHER_THREAD)
  display.disableAutoLayout()

  var pipe = display.newViewerPipe()
  pipe.addViewerListener(this)
  pipe.addSink(graph)

  val pumper = new Thread(
    new Runnable() {
      def run() {
        while(true) {
          pipe.pump()
          Thread.sleep(50)
        }
      }
    }
  )

  pumper.start()

  var view = display.addDefaultView(false)

  var label = new TextArea("Click node for info.\nScroll with arrow keys.\nZoom with PgDown and PgUp.")
  label.editable = false
  label.background = java.awt.Color.LIGHT_GRAY

  var scrollPane = new ScrollPane()
  scrollPane.viewportView = label

  var frame = new MainFrame {
    title = "DDG Debug"
    contents = new GridBagPanel() {
      val c = new Constraints()
      c.gridx = 0
      c.gridy = 0
      c.weightx = 1
      c.weighty = 1
      c.fill = Fill.Both
      layout(Component.wrap(view)) = c
      c.gridx = 0
      c.gridy = 1
      c.weightx = 1
      c.weighty = 0.2
      c.fill = Fill.Both
      layout(scrollPane) = c
    }
    size = new Dimension(800, 600)
    visible = true
  }

  val pos = new HashMap[Node, (Int, Int)]()
  val nodes = new ListBuffer[Node]()
  val idToNodes = new HashMap[String, Node]()

  private def setPos(node: Node, x: Int, y: Int) {
    findNode(node).setAttribute("xyz", x.asInstanceOf[AnyRef],
                             y.asInstanceOf[AnyRef],
                             0.asInstanceOf[AnyRef])

    pos(node) = (x, y)
  }

  private def getPos(node: Node): (Int, Int) = {
    pos(node)
  }

  private def setLabel(node: Node, label: String) {
    findNode(node).addAttribute("ui.label", label)
  }

  private def setClass(node: Node, cssClass: String) {
    findNode(node).addAttribute("ui.class", cssClass)
  }

  private def setStyle(node: Node, style: String) {
    findNode(node).addAttribute("ui.style", style)
  }

  private def addEdge(id1: String, id2: String): org.graphstream.graph.Edge = {
    graph.addEdge(id1 + " -> " + id2, id1, id2)
  }

  private def removeEdge(id1: String, id2: String) = {
    graph.removeEdge(id1 + " -> " + id2)
  }

  private def addNode(node: Node): org.graphstream.graph.Node = {
    nodes += node
    idToNodes += (System.identityHashCode(node).toString() -> node)
    graph.addNode(System.identityHashCode(node).toString())
  }

  private def removeNode(node: Node, removeFromSet:Boolean = true) {
    if(removeFromSet) {
      nodes -= node
    }
    idToNodes -= System.identityHashCode(node).toString()
    graph.removeNode(System.identityHashCode(node).toString())
  }

  private def addEdge(a: Node, b: Node): org.graphstream.graph.Edge = {
    addEdge(System.identityHashCode(a).toString(),
            System.identityHashCode(b).toString())
  }

  private def removeEdge(a: Node, b: Node) {
    removeEdge(a, b);
  }

  private def findEdge(a: Node, b: Node): org.graphstream.graph.Edge = {
    graph.getEdge(System.identityHashCode(a).toString() + " -> " +
            System.identityHashCode(b).toString())
  }

  private def findNode(node: Node): org.graphstream.graph.Node = {
    graph.getNode(System.identityHashCode(node).toString())
  }

  def createTree(node: Node, parent: Node) {
    val existing = findNode(node)
    nodesToKeep(node) = true

    if(existing == null) {
      addNode(node)
      setStyle(node, "stroke-mode: plain;")


    } else {
      setStyle(node, "stroke-mode: none;")
    }

    if(parent != null && findEdge(parent, node) == null) {
      addEdge(parent, node)
    }

    val nodeType = getNodeType(node)
    if(showLabels) {
      val parameterInfo = getParameterInfo(node)

      val methodName = extractMethodName(node)

      setLabel(node, nodeType + " " + parameterInfo + " in " + methodName)
    }
    setClass(node, nodeType)

    node.children.foreach(x => {
        createTree(x, node)
    })
  }

  //Layouts the tree and returns the subtree width.
  def layoutTree(parent: Node, depth: Int): Int = {

    setPos(parent, 0, depth * vSpacing)

    if(parent.children.length == 0) {
      return 0
    }
    else {
      var sum = 0;
      parent.children.foreach(children => {
          val width = layoutTree(children, depth + 1)
          if(sum != 0)
            translateTree(children, sum)

          sum += (width + hSpacing)
      })

      sum -= hSpacing

      setPos(parent, sum / 2, depth * vSpacing)

      sum
    }
  }

  def translateTree(parent: Node, dx: Int) {

    val (x, y) = getPos(parent)
    setPos(parent, x + dx, y)

    if(((x + dx) / vSpacing) % 2 == 0) {
      setStyle(parent, "text-offset: 0, 3;")
    } else {
      setStyle(parent, "text-offset: 0, -27;")
    }

    parent.children.foreach(child => {
        translateTree(child, dx)
    })
  }


  var nodesToKeep: HashMap[Node, Boolean] = null
  var markedForRemoval: List[Node] = null

  def showDDG(root: RootNode) = {

    nodesToKeep = new HashMap[Node, Boolean]()

    nodes.foreach(x => nodesToKeep(x) = false)

    createTree(root, null)
    layoutTree(root, 0)

    if(markedForRemoval != null && highlightRemoved)
      markedForRemoval.foreach(x => removeNode(x, false))

    markedForRemoval = List()

    nodesToKeep.foreach(pair => {
      if(!pair._2) {
        if(highlightRemoved) {
          markedForRemoval = pair._1 :: markedForRemoval
          nodes -= pair._1
          setStyle(pair._1, "stroke-mode: plain;")
          setStyle(pair._1, "stroke-color: red;")
        } else {
          removeNode(pair._1)
        }
      }
    })
  }

  private def getNodeType(node: Node): String = {
    node match {
      case x:WriteNode => "write"
      case x:ReadNode => "read"
      case x:MemoNode => "memo"
      case x:ParNode => "par"
      case x:RootNode => "root"
    }
  }

  private def getParameterInfo(node: Node): String = {
    node match {
      case x:WriteNode => x.mod.toString
      case x:ReadNode => x.mod.toString
      case x:MemoNode => x.signature.toString
      case x:ParNode => ""
      case x:RootNode => ""
    }
  }

  private def extractMethodName(node: Node): String = {

    if(node.stacktrace == null) {
      return "<No stacktrace available. Set Main.debug = true to enable stacktraces>"
    }

    val methodNames = node.stacktrace.map(y => y.getMethodName())
    val fileNames = node.stacktrace.map(y => (y.getMethodName(), y.getFileName(), y.getLineNumber()))

    var (_, fileName, lineNumber) = fileNames.filter(y => (y._1.contains("apply")))(0)

    var currentMethod = methodNames.filter(y => (!y.startsWith("<init>")
                                            && !y.startsWith("()")
                                            && !y.startsWith("addRead")
                                            && !y.startsWith("addWrite")
                                            && !y.startsWith("addMemo")
                                            && !y.startsWith("createMod")
                                            && !y.startsWith("getStackTrace")
                                            && !y.startsWith("apply")
                                            && !y.startsWith("read")
                                            && !y.startsWith("memo")
                                            && !y.startsWith("par")
                                            && !y.startsWith("write")
                                            && !y.startsWith("mod")))(0)

    if(currentMethod.contains("$")) {
      currentMethod = currentMethod.substring(0, currentMethod.lastIndexOf("$"))
      currentMethod = currentMethod.substring(currentMethod.lastIndexOf("$") + 1)
    }

    if(methodNames.find(x => x == "createMod").isDefined) {
      currentMethod += " (createMod)"
    }

    currentMethod + " " + fileName + ":" + lineNumber.toString
  }

  def viewClosed(id: String) {

  }

  def buttonPushed(id: String) {
      val node = idToNodes(id)
      label.text = getNodeType(node) + " " + getParameterInfo(node) + "\nIn " + extractMethodName(node)
  }

  def buttonReleased(id: String) {

  }
}

