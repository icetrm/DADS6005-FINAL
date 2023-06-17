import pygame
import random
import math

# # initialize pygame
# pygame.init()

# # Initialize Global variables
# screenSize = (800, 600)
# background_color = (0, 0, 255)
# # Initialize screen
# screen = pygame.display.set_mode(screenSize)

# Define Classes

class SpaceObject:

	def __init__(self, image, posX=0, posY=0, speedX = 0, speedY = 0, sizeX = 64, sizeY = 64):
		#self.namme	= name
		self.image  = image
		self.sizeX  = sizeX
		self.sizeY  = sizeY
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

class Bullet(SpaceObject):


	def update_bullet_position(self, screenSize):

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

def is_collision(object1, object2):

	obj1_midX = object1.posX + object1.sizeX
	obj1_midY = object1.posY + object1.sizeY
	obj2_midX = object2.posX + object2.sizeX
	obj2_midY = object2.posY + object2.sizeY

	# think if I want to improve this...
	distance = math.sqrt(math.pow(obj1_midX-obj2_midX,2) + math.pow(obj1_midY-obj2_midY,2))
	collision_limit = (object1.sizeX + object1.sizeY + object2.sizeX + object2.sizeY) / 4

	return distance < collision_limit

def game_over_show(screenSize):
	
	# Move enemies below screen (is there a better way?)
	for i in range(num_of_enemies):
		enemies[i].posY = screenSize[1] + 100

	# Game over font
	game_over_font = pygame.font.Font('freesansbold.ttf', 64)
	game_over_text = game_over_font.render('GAME OVER', True, (255, 0, 0))
	screen.blit(game_over_text, (screenSize[0]/2-200, screenSize[1]/2-75))


#########################
####   Main Program   ###
#########################
#if __name__ == '__main__':

# initialize pygame
pygame.init()

# Initialize Global variables
screenSize = (800, 600)
background_color = (0, 0, 0)
# Initialize screen
screen = pygame.display.set_mode(screenSize)

# Initialize images
#example_image     = pygame.image.load('.\\images\\') 
icon_image			= pygame.image.load('.\\images\\space-invader-icon.png') 
background_image	= pygame.image.load('.\\images\\Google-Map-solar-system.jpg')
player_image		= pygame.image.load('.\\images\\space-ship.png')
bullet_image		= pygame.image.load('.\\images\\bullet.png')
enemy_images	    = [pygame.image.load('.\\images\\enemy.png'), 
						pygame.image.load('.\\images\\yoda.png')]

# Initialize sounds
bullet_sound		= pygame.mixer.Sound('.\\sounds\\laser.wav')
explosion_sound		= pygame.mixer.Sound('.\\sounds\\explosion.wav')
pygame.mixer.music.load('.\\sounds\\background.wav')
pygame.mixer.music.play(-1)


# Initialize settings
player_maxSpeedX = 3
player_maxSpeedY = 3
enemy_maxSpeedX = 2
enemy_maxSpeedY = 2

# Game settings
num_of_enemies		= 3
score				= 0
level				= 1

# initialize other variables
keyX_pressed = 0
keyY_pressed = 0
game_over 	 = False

# initialize player
player = SpaceShip(player_image, screenSize[0]/2-32, screenSize[1]-100)

# initialize enemies
enemies = []
for i in range(num_of_enemies):
	enemies.append(SpaceEnemy(enemy_images[0], random.randint(0, screenSize[0]), random.randint(-500, -100), speedY = level))

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

			#if event.key == pygame.K_SPACE and bullet.state == 'ready':
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

	# Move player and check not out of screen
	player.update_player_postion(screenSize)


	if game_over:
		game_over_show(screenSize)
		# add explosion picutre here
	else:
		# Move enemies and check collisions
		for i in range(num_of_enemies):
			enemies[i].update_enemy_position(screenSize)
			enemies[i].show()
			if is_collision(enemies[i], player):
				explosion_sound.play()
				game_over = True
				break
			enemies[i].show()

		# show player
		player.show()
	


	pygame.display.update()




print('Successfully quit!')