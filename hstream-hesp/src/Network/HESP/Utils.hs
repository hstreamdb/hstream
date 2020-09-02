module Network.HESP.Utils
  ( pairs
  ) where

pairs :: [a] -> [(a, a)]
pairs (x0:x1:xs) = (x0, x1) : pairs xs
pairs []         = []
pairs _          = error "elements must be even"
