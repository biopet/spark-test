package nl.biopet.tools.sparktest

import java.io.File

case class Args(inputFile: File = null,
                reference: File = null,
                outputDir: File = null,
                withCache: Boolean = false,
                sparkMaster: String = null,
                maxIterations: Option[Int] = None)
