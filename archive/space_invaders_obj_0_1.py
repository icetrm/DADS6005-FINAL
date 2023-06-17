import pygame
import random

# # initialize pygame
# pygame.init()

# # Initialize Global variables
# screenSize = (800, 600)
# background_color = (0, 0, 255)
# # Initialize screen
# screen = pygame.display.set_mode(screenSize)

# Define Classes

class SpaceObject:

	def __init__(self, image, posX=0, posY=0, speedX = 0, speedY = 0):
		#self.namme	= name
		self.image  = image
		self.sizeX  = 64
		self.sizeY  = 64
		self.posX = posX
		self.posY = posY
		self.speedX = speedX
		self.speedY	= speedY

	def show(self):
		screen.blit(self.image, (self.posX, self.posY))



class SpaceShip(SpaceObject):
    
    # def __init__(self):
    #     super().__init__()

	def update_player_postion(self, screenSize):

		# Update X position
		self.posX += self.speedX
		if self.posX < 0:
			self.posX = 0
		elif self.posX > screenSize[0]-self.sizeX:
			self.posX = screenSize[0]-self.sizeX

		# Update Y position
		self.posY += self.speedY
		if self.posY < 0:
			self.posY = 0
		elif self.posY > screenSize[0]-self.sizeY:
			self.posY = screenSize[0]-self.sizeY


class SpaceEnemy(SpaceObject):

	def update_enemy_position(self, screenSize):

		# Update X position
		self.posX += self.speedX
		# if self.posX < 0:
		# 	self.posX = 0
		# elif self.posX > screenSize[0]-self.sizeX:
		# 	self.posX = screenSize[0]-self.sizeX

		# Update Y position
		self.posY += self.speedY
		# if self.posY < 0:
		# 	self.posY = 0
		# elif self.posY > screenSize[0]-self.sizeY:
		# 	self.posY = screenSize[0]-self.sizeY


# Define Procedures

def isCollision():
	pass






#########################
####   Main Program   ###
#########################
#if __name__ == '__main__':

# initialize pygame
pygame.init()

# Initialize Global variables
screenSize = (1200, 700)
background_color = (0, 0, 0)
# Initialize screen
screen = pygame.display.set_mode(screenSize)

# Initialize images
#example_image     = pygame.image.load('.\\images\\') 
icon_image			= pygame.image.load('.\\images\\space-invader-icon.png') 
background_image	= pygame.image.load('.\\images\\Google-Map-solar-system.jpg')
player_image		= pygame.image.load('.\\images\\space-ship.png')
bullet_image		= pygame.image.load('.\\images\\bullet.png')
enemy_image			= pygame.image.load('.\\images\\yoda.png')

# Initialize sounds
pygame.mixer.music.load('.\\sounds\\background.wav')
bullet_sound		= pygame.mixer.Sound('.\\sounds\\laser.wav')
explosion_sound		= pygame.mixer.Sound('.\\sounds\\explosion.wav')

# Initialize settings
player_maxSpeedX = 3
player_maxSpeedY = 3
enemy_maxSpeedX = 2
enemy_maxSpeedY = 2

# Game settings
difficulty_level	= 3
num_of_enemies		= 3
score				= 0
level				= 1

# initialize other variables
keyX_pressed = 0
keyY_pressed = 0

# initialize player
player = SpaceShip(player_image, 400, 100)

# initialize enemies
enemies = []
for i in range(num_of_enemies):
	enemies.append(SpaceEnemy(enemy_image, random.randint(0, screenSize[0]), -70, speedY = 2))

# --------------
# Main Game Loop
# --------------
running = True
while running:
	screen.fill(background_color)
	screen.blit(background_image, (0,0))

	for event in pygame.event.get():
		if event.type == pygame.QUIT:
			running = False	

		# if key is pressed
		if event.type == pygame.KEYDOWN:
			if event.key == pygame.K_LEFT:
				player.speedX = -player_maxSpeedX
				keyX_pressed += 1
			if event.key == pygame.K_RIGHT:
				player.speedX = player_maxSpeedX
				keyX_pressed += 1
			if event.key == pygame.K_UP:
				player.speedY = -player_maxSpeedY
				keyY_pressed += 1
			if event.key == pygame.K_DOWN:
				player.speedY = player_maxSpeedY
				keyY_pressed += 1


			# if event.key == pygame.K_SPACE and bullet.state == 'ready':
			# 	bullet_sound = mixer.Sound('laser.wav')
			# 	bullet_sound.play()
			# 	bulletX = playerX
			# 	bulletY = playerY
			# 	fire_bullet(bulletX, bulletY)

		if event.type == pygame.KEYUP:
			if event.key == pygame.K_LEFT or event.key == pygame.K_RIGHT:
				keyX_pressed -= 1
				if keyX_pressed == 0:
					player.speedX = 0
			if event.key == pygame.K_UP or event.key == pygame.K_DOWN:
				keyY_pressed -= 1
				if keyY_pressed == 0:
					player.speedY = 0


	for i in range(num_of_enemies):
		enemies[i].update_enemy_position(screenSize)
		enemies[i].show()

	
	# Move player and check not out of screen
	player.update_player_postion(screenSize)
	
	player.show()
	enemies[0].show()

	pygame.display.update()


print('Successfully quit!')