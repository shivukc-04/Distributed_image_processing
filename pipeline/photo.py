
import cv2, numpy as np

# 1. 2048x2048 high-res test pattern (16 tiles total)
img_2k = np.random.randint(0, 256, (2048, 2048, 3), dtype=np.uint8)
cv2.imwrite('test_2k.jpg', img_2k)

# 2. 4096x4096 ultra high-res test pattern (64 tiles total)
img_4k = np.random.randint(0, 256, (4096, 4096, 3), dtype=np.uint8)
cv2.imwrite('test_4k.jpg', img_4k)

print('Generated test_2k.jpg (2048x2048) and test_4k.jpg (4096x4096)')

