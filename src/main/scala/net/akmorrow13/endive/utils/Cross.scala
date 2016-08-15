package net.akmorrow13.endive.utils
import scala.collection.Set
/* Borrowed from http://stackoverflow.com/questions/2725682/cross-product-of-2-sets-in-scala */

class
Crossable[E1](es1: Traversable[E1])
{
    def
    ×[E2](es2: Traversable[E2]): Traversable[(E1, E2)] =
      for (e1 <- es1; e2 <- es2) yield (e1, e2)

    def
    cross[E2](es2: Traversable[E2]): Traversable[(E1, E2)] =
      for (e1 <- es1; e2 <- es2) yield (e1, e2)
}


object
Crossable
{
    implicit
    def
    trav2Crossable[E1](es1: Traversable[E1]): Crossable[E1] =
        new Crossable[E1](es1)
}
