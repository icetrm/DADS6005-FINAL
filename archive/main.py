import pygame
from pygame import mixer
import random
import math

# initialize pygame
pygame.init()

# create the screen and background image
screenSize = (800,600)
screen = pygame.display.set_mode(screenSize)
background = pygame.image.load('Google-Map-solar-system.jpg')

# Backgound sound
mixer.music.load('background.wav')
mixer.music.play(-1)

# Title and icon
pygame.display.set_caption('Space Invaders')
icon = pygame.image.load('space-invader-icon.png')
pygame.display.set_icon(icon)

# Scope
score_value = 0
font = pygame.font.Font('freesansbold.ttf', 32)
textX = 10
textY = 10

# Game over font
over_font = pygame.font.Font('freesansbold.ttf', 64)


# Player
playerImg = pygame.image.load('space-ship.png')
playerSize = 64
playerX = 370
playerY = 480
playerX_change = 0
playerY_change = 0
key_pressed = 0

# Enemy
enemyImg = []
#enemyImg = pygame.image.load('outer-space-alien.png')
enemySize = 64
enemyX = []
enemyY = []
enemyX_change = []
enemyY_change = []
num_of_enemies = 6

for i in range(num_of_enemies):
	enemyImg.append(pygame.image.load('yoda.png'))
	enemyX.append(random.randint(0, screenSize[0]-enemySize))
	enemyY.append(random.randint(50, screenSize[1] / 3))
	enemyX_change.append(4)
	enemyY_change.append(20)

# bullet
bulletImg = pygame.image.load('bullet.png')
bulletSize = 32
bulletX = playerX
bulletY = playerY
bulletX_change = 0
bulletY_change = 10
bullet_state = 'ready' 

def show_score(x,y):
	score = font.render("Score : " + str(score_value), True, (0, 0, 255))
	screen.blit(score, (x, y))

def game_over_text():
	over_text = over_font.render('GAME OVER', True, (255, 0, 0))
	screen.blit(over_text, (200, 250))

def player(x, y):
	screen.blit(playerImg, (int(x), int(y)))

def enemy(x, y, i):
	screen.blit(enemyImg[i], (int(x), int(y)))

def fire_bullet(x, y):
	global bullet_state
	bullet_state = 'fire'
	screen.blit(bulletImg, (int(x)+16, int(y)+10))

def isCollision(enemyX, enemyY, bulletX, bulletY):
	distance = math.sqrt(math.pow(enemyX-bulletX,2) + math.pow(enemyY-bulletY,2))
	return distance < 27




# Game loop
running = True
while running:
	screen.fill((0, 0, 255))
	screen.blit(background, (0,0))

	for event in pygame.event.get():
		if event.type == pygame.QUIT:
			running = False
	
		# if key is pressed
		if event.type == pygame.KEYDOWN:
			if event.key == pygame.K_LEFT:
				playerX_change = -5
				key_pressed += 1
			if event.key == pygame.K_RIGHT:
				playerX_change = 5
				key_pressed += 1
			if event.key == pygame.K_SPACE and bullet_state == 'ready':
				bullet_sound = mixer.Sound('laser.wav')
				bullet_sound.play()
				bulletX = playerX
				bulletY = playerY
				fire_bullet(bulletX, bulletY)

		if event.type == pygame.KEYUP:
			if event.key == pygame.K_LEFT or event.key == pygame.K_RIGHT:
				key_pressed -= 1
				if key_pressed == 0:
					playerX_change = 0

	
	# Move player and check not out of screen
	playerX += playerX_change
	if playerX < 0:
		playerX = 0
	elif playerX > screenSize[0]-playerSize:
		playerX = screenSize[0]-playerSize

	# Bullet movement
	if bulletY < 0:
		bulletY = playerY
		bullet_state = 'ready'

	if bullet_state == "fire":
		fire_bullet(bulletX, bulletY)
		bulletY -= bulletY_change
	
	# Move enemy

	for i in range(num_of_enemies):
		
		if enemyY[i] > 400:
			for j in range(num_of_enemies):
				enemyY[j] = screenSize[1] + 100
			game_over_text()
			break	

		if enemyX[i] < 0:
			enemyX[i] = 0
			enemyX_change[i] = -enemyX_change[i]
			enemyY[i] += enemyY_change[i]
		elif enemyX[i] > screenSize[0]-enemySize:
			enemyX[i] = screenSize[0]-enemySize
			enemyX_change[i] = -enemyX_change[i]
			enemyY[i] += enemyY_change[i]
	
		# Check for collistion
		collistion = isCollision(enemyX[i], enemyY[i], bulletX, bulletY)
		if collistion:
			explosion_sound = mixer.Sound('laser.wav')
			explosion_sound.play()
			bulletY = playerY
			bullet_state = 'ready'
			score_value += 1
			enemyX[i] = random.randint(0, screenSize[0]-enemySize)
			enemyY[i] = random.randint(50, screenSize[1] / 3)

		# Show enemy
		enemyX[i] += enemyX_change[i]
		enemy(enemyX[i], enemyY[i], i)

	# Show player and score
	player(playerX, playerY)
	show_score(textX, textY)

	# Update screen
	pygame.display.update()
